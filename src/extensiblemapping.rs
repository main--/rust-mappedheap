use libc::{mmap, munmap, PROT_READ, PROT_WRITE, MAP_SHARED, c_int, off_t, c_void, MAP_FAILED};
use std::fs::File;
use std::io::{Write, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::{mem, ptr, cmp};
use std::cell::Cell;
use std::usize;

use maybe_mut::MaybeMut;
use futex::raw::Mutex;

fn do_mmap(fd: c_int, offset: off_t, length: usize, fixed_addr: Option<usize>) -> Option<usize> {
    let ret = unsafe {
        mmap(fixed_addr.map(|x| x as *mut c_void).unwrap_or(ptr::null_mut()),
             length,
             PROT_READ | PROT_WRITE,
             MAP_SHARED,
             fd, offset)
    };
    
    if ret == MAP_FAILED {
        None
    } else if fixed_addr.map(|x| x != ret as usize).unwrap_or(false) {
        unsafe { munmap(ret, length) };
        None
    } else {
        Some(ret as usize)
    }
}

pub const PAGESZ: usize = 4096;
const MAGIC: &[u8; 8] = b"fuckfuck";

pub struct ExtensibleMapping {
    file: File,
    addr: usize,
    size: Cell<u64>,
}

impl ExtensibleMapping {
    fn header(&self) -> &mut FileHeader {
        unsafe { &mut *(self.addr as *mut FileHeader) }
    }

    pub fn initialize(file: &mut File) {
        let header = FileHeader {
            magic: *MAGIC,
            size: 2,
            _pad0: [0; 48],
            resize_lock: Mutex::new(),
            _pad1: [0; 60],
            alloc_lock: Mutex::new(),
            freelist_id: 1,
            _pad2: [0; 48],
            _pad_end: [0; HEADER_PAD_END],
        };
        assert_eq!(mem::size_of_val(&header), PAGESZ);
        
        let buffer = (&header) as *const _ as *const [u8; PAGESZ];
        file.set_len(0).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(unsafe { &*buffer }).unwrap();
        file.write_all(&[0u8; PAGESZ]).unwrap();
    }
    
    pub fn open(file: File) -> ExtensibleMapping {
        let len = file.metadata().unwrap().len();
        assert!(len <= usize::MAX as u64);

        let size = len / (PAGESZ as u64); // round down to full pages
        assert!(size > 0);
        
        let addr = do_mmap(file.as_raw_fd(), 0, size as usize * PAGESZ, None).unwrap();
        
        ExtensibleMapping { file, addr, size: Cell::new(size) }.sanity_check()
    }

    fn sanity_check(self) -> ExtensibleMapping {
        // TODO: remove this idk - it's racy
        // A scenario where someone else is resizing RIGHT NOW is valid!
        debug_assert_eq!(self.header().size, self.size.get());

        assert_eq!(&self.header().magic, MAGIC);
        self
    }

    pub fn page(&self, id: PageId) -> Option<*mut [u8; PAGESZ]> {
        if id == 0 || id >= self.size.get() {
            None
        } else {
            Some(((self.addr + id as usize * PAGESZ) as *mut [u8; PAGESZ]))
        }
    }

    pub unsafe fn page_mut<T>(&self, id: PageId) -> Option<&mut T> {
        assert_eq!(PAGESZ, mem::size_of::<T>());
        self.page(id).map(|x| &mut *(x as *mut T))
    }
    
    /// Attempts to double the file size.
    /// Once this returns, the file will always be at least twice as large.
    ///
    /// To use it, you have to update the mapping with `try_grow_mapping_inplace` or `remap`!
    pub fn grow_file(&self) {
        let target = self.size.get() * 2;
        let header = self.header();
        header.resize_lock.acquire();
        if header.size < target {
            header.size = target;
            self.file.set_len(target * (PAGESZ as u64)).unwrap();
        }
        header.resize_lock.release();
    }

    #[cfg(target_os = "linux")]
    pub fn try_grow_mapping_inplace(&self) -> bool {
        // On linux, we can just use mremap.
        use libc::mremap;

        let newsize = self.header().size;
        let ret = unsafe { mremap(self.addr as *mut c_void,
                                  self.size.get() as usize * PAGESZ,
                                  newsize as usize * PAGESZ,
                                  0) };
        if ret == MAP_FAILED {
            false
        } else {
            self.size.set(newsize);
            true
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn try_grow_mapping_inplace(&self) -> bool {
        // Portable version: We just try to map the new part right after the existing mapping.
        let size = self.size.get();
        let newsize = self.header().size;

        // TODO: cast might be bug
        if let Some(_) = do_mmap(self.file.as_raw_fd(),
                                 (size as usize * PAGESZ) as i64,
                                 (newsize - size) as usize * PAGESZ,
                                 Some(self.addr + size as usize * PAGESZ)) {
            self.size.set(newsize);
            true
        } else {
            false
        }
    }

    #[cfg(target_os = "linux")]
    pub fn remap(&mut self) {
        // On linux, we can (again) just use mremap.
        use libc::{mremap, MREMAP_MAYMOVE};

        let newsize = self.header().size;
        let ret = unsafe { mremap(self.addr as *mut c_void,
                                  self.size.get() as usize * PAGESZ,
                                  newsize as usize * PAGESZ,
                                  MREMAP_MAYMOVE) };
        assert!(ret != MAP_FAILED);
        self.addr = ret as usize;
        self.size.set(newsize);
    }
    
    #[cfg(not(target_os = "linux"))]
    pub fn remap(&mut self) {
        // Portable version: Unmap, then map.

        let newsize = self.header().size;
        let ret = unsafe { munmap(self.addr as *mut c_void, self.size.get() as usize * PAGESZ) };
        assert!(ret == 0);
        self.addr = do_mmap(self.file.as_raw_fd(), 0, newsize as usize * PAGESZ, None).unwrap();
        self.size.set(newsize);
    }

    pub fn alloc(&mut self) -> PageId {
        ExtensibleMapping::do_alloc(self.into()).unwrap()
    }

    pub fn try_alloc(&self) -> Option<PageId> {
        ExtensibleMapping::do_alloc(self.into())
    }
    
    pub fn do_alloc(mut this: MaybeMut<Self>) -> Option<PageId> {
        this.header().alloc_lock.acquire();

        let ret;
        if this.header().freelist_id == NULL_PAGE {
            // slow path :(
            ret = this.size.get();
            this.grow_file();
            if !this.try_grow_mapping_inplace() {
                if let Some(this) = this.borrow_mut() {
                    this.remap();
                } else {
                    return None;
                }
            }

            let header = this.header();
            // inclusive start, exclusive end
            let mut first_free: PageId = ret + 1; // we allocated the first page, everything after is free game
            let mut last_free: PageId = this.size.get();
            while first_free != last_free {
                last_free -= 1;
                let pid = last_free;
                
                let page: &mut FreelistPage = unsafe { this.page_mut(pid).unwrap() };
                page.n_entries = cmp::min(last_free - first_free, FREELIST_E_PER_PAGE as u64);
                for (i, e) in page.entries.iter_mut().enumerate().take(page.n_entries as usize) {
                    *e = i as u64 + first_free;
                }
                page.next = header.freelist_id;
                header.freelist_id = pid;
                first_free += page.n_entries;
            }
        } else {
            let header = this.header();
            let freelist: &mut FreelistPage = unsafe { this.page_mut(header.freelist_id).unwrap() };
            if freelist.n_entries == 0 {
                // consume this page
                ret = header.freelist_id;
                header.freelist_id = freelist.next;
            } else {
                freelist.n_entries -= 1;
                ret = freelist.entries[freelist.n_entries as usize];
            }
        }
        this.header().alloc_lock.release();
        Some(ret)
    }

    pub fn free(&self, id: PageId) {
        assert!(id < self.size.get());

        let header = self.header();
        header.alloc_lock.acquire();

        if header.freelist_id != NULL_PAGE {
            // try appending to existing freelist page
            let freelist: &mut FreelistPage = unsafe { self.page_mut(header.freelist_id) }.unwrap();
            if freelist.n_entries < freelist.entries.len() as u64 {
                freelist.entries[freelist.n_entries as usize] = id;
                freelist.n_entries += 1;
                // added to freelist, so we can free it in the file
                clear_page(self.addr as usize + ((id as usize) * PAGESZ));
                header.alloc_lock.release();
                return;
            }
        }
        
        // link in at front
        let freelist: &mut FreelistPage = unsafe { self.page_mut(id) }.unwrap();
        freelist.n_entries = 0;
        freelist.next = header.freelist_id;
        header.freelist_id = id;
        header.alloc_lock.release();
    }
}

const FREELIST_E_PER_PAGE: usize = (PAGESZ / 8) - 2;

#[repr(C)]
struct FreelistPage {
    n_entries: u64,
    entries: [PageId; FREELIST_E_PER_PAGE],
    next: PageId,
}

pub type PageId = u64;
pub const NULL_PAGE: PageId = 0;

const HEADER_PAD_END: usize = PAGESZ - 64 * 3;

#[repr(C)]
struct FileHeader {
    magic: [u8; 8],
    size: PageId, // number of pages
    _pad0: [u8; 48],
    resize_lock: Mutex,
    _pad1: [u8; 60],
    alloc_lock: Mutex,
    freelist_id: PageId,
    _pad2: [u8; 48],
    _pad_end: [u8; HEADER_PAD_END],
}


#[cfg(target_os = "linux")]
fn clear_page(addr: usize) {
    use libc::{madvise, MADV_REMOVE};
    unsafe {
        madvise(addr as *mut c_void, PAGESZ, MADV_REMOVE);
    }
}

#[cfg(not(target_os = "linux"))]
fn clear_page(_: usize) {
    // unimplemented, do nothing
    // sorry, your space is wasted
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    
    #[test]
    fn it_works() {
        let mut file = OpenOptions::new().read(true).write(true).open("map.bin").unwrap();
        ExtensibleMapping::initialize(&mut file);
        let mut mapping = ExtensibleMapping::open(file);

        assert_eq!(mapping.size.get(), 2);
        assert_eq!(mapping.alloc(), 1);
        assert_eq!(mapping.size.get(), 2);
        assert_eq!(mapping.alloc(), 2);
        assert_eq!(mapping.size.get(), 4);
        assert_eq!(mapping.alloc(), 3);
        assert_eq!(mapping.size.get(), 4);
        mapping.free(1);
        assert_eq!(mapping.alloc(), 1);
        mapping.free(1);
        mapping.free(2);
        mapping.free(3);
        mapping.alloc();
        mapping.alloc();
        mapping.alloc();
        assert_eq!(mapping.size.get(), 4);
        assert_eq!(mapping.alloc(), 4);
        assert_eq!(mapping.size.get(), 8);
    }
}
