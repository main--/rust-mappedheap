use libc::{mmap, munmap, PROT_READ, PROT_WRITE, MAP_SHARED, c_int, off_t, c_void, MAP_FAILED};
use std::fs::File;
use std::io::{Write, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::{mem, ptr, cmp};
use std::cell::Cell;
use std::usize;

use futex::raw::Mutex;
use futex::RwLock;

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
    } else {
        Some(ret as usize)
    }
}

pub const PAGESZ: usize = 4096;
const MAGIC: &[u8; 8] = b"fuckfuck";

pub struct ExtensibleMapping {
    file: File,
    header_ptr: *mut FileHeader,
    fragments: RwLock<Vec<Fragment>>,
}

struct Fragment {
    addr: usize,
    offset: u64,
    size: Cell<u64>,
}

impl Fragment {
    fn grow(&self, file: &File, additional: u64) -> Option<Fragment> {
        let size = self.size.get();
        let addr_desired = self.addr + size as usize * PAGESZ;

        let addr = do_mmap(file.as_raw_fd(),
                           (size as usize * PAGESZ) as i64,
                           additional as usize * PAGESZ,
                           Some(addr_desired)).unwrap();
        if addr == addr_desired {
            self.size.set(size + additional);
            None
        } else {
            Some(Fragment {
                addr: addr,
                offset: self.offset + size,
                size: Cell::new(additional),
            })
        }
    }
}

impl Drop for Fragment {
    fn drop(&mut self) {
        unsafe {
            munmap(self.addr as *mut _, self.size.get() as usize * PAGESZ);
        }
    }
}

impl ExtensibleMapping {
    fn header(&self) -> &mut FileHeader {
        unsafe { &mut *self.header_ptr }
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
        
        ExtensibleMapping {
            file,
            header_ptr: addr as *mut _,
            fragments: RwLock::new(vec![Fragment { addr, offset: 0, size: Cell::new(size) }]),
        }.sanity_check()
    }

    fn sanity_check(self) -> ExtensibleMapping {
        assert_eq!(&self.header().magic, MAGIC);
        self
    }

    pub fn page(&self, id: PageId) -> Option<*mut [u8; PAGESZ]> {
        if id == NULL_PAGE || id >= self.header().size {
            return None;
        }

        let mut fragments = self.fragments.read();
        let mut index = match fragments.binary_search_by_key(&id, |x| x.offset) {
            Ok(i) => i,
            Err(i) => i - 1,
        };

        if id - fragments[index].offset >= fragments[index].size.get() {
            // need more mapping
            drop(fragments);

            let mut m_fragments = self.fragments.write();
            if id - m_fragments[index].offset >= m_fragments[index].size.get() {
                let mapsize: u64 = m_fragments.iter().map(|x| x.size.get()).sum();
                let required = self.header().size - mapsize;
                assert!(required > 0);
                if let Some(x) = m_fragments.last().unwrap().grow(&self.file, required) {
                    m_fragments.push(x);
                    index += 1;
                }
            }
            drop(m_fragments);
            
            fragments = self.fragments.read();
        }

        let fragment = &fragments[index];
        assert!(id - fragment.offset < fragment.size.get());
        Some(((fragment.addr + (id - fragment.offset) as usize * PAGESZ) as *mut [u8; PAGESZ]))
    }

    pub unsafe fn page_mut<T>(&self, id: PageId) -> Option<&mut T> {
        assert_eq!(PAGESZ, mem::size_of::<T>());
        self.page(id).map(|x| &mut *(x as *mut T))
    }
    
    pub fn double_file(&self) {
        let header = self.header();
        header.resize_lock.acquire();
        header.size *= 2;
        self.file.set_len(header.size * (PAGESZ as u64)).unwrap();
        header.resize_lock.release();
    }

    pub fn alloc(&self) -> PageId {
        self.header().alloc_lock.acquire();

        let ret;
        if self.header().freelist_id == NULL_PAGE {
            // slow path :(
            ret = self.header().size;
            self.double_file();

            let header = self.header();
            // inclusive start, exclusive end
            let mut first_free: PageId = ret + 1; // we allocated the first page, everything after is free game
            let mut last_free: PageId = self.header().size;
            while first_free != last_free {
                last_free -= 1;
                let pid = last_free;
                
                let page: &mut FreelistPage = unsafe { self.page_mut(pid).unwrap() };
                page.n_entries = cmp::min(last_free - first_free, FREELIST_E_PER_PAGE as u64);
                for (i, e) in page.entries.iter_mut().enumerate().take(page.n_entries as usize) {
                    *e = i as u64 + first_free;
                }
                page.next = header.freelist_id;
                header.freelist_id = pid;
                first_free += page.n_entries;
            }
        } else {
            let header = self.header();
            let freelist: &mut FreelistPage = unsafe { self.page_mut(header.freelist_id).unwrap() };
            if freelist.n_entries == 0 {
                // consume self page
                ret = header.freelist_id;
                header.freelist_id = freelist.next;
            } else {
                freelist.n_entries -= 1;
                ret = freelist.entries[freelist.n_entries as usize];
            }
        }
        self.header().alloc_lock.release();
        ret
    }

    pub fn free(&self, id: PageId) {
        assert!(id < self.header().size);

        let header = self.header();
        header.alloc_lock.acquire();

        if header.freelist_id != NULL_PAGE {
            // try appending to existing freelist page
            let freelist: &mut FreelistPage = unsafe { self.page_mut(header.freelist_id) }.unwrap();
            if freelist.n_entries < freelist.entries.len() as u64 {
                freelist.entries[freelist.n_entries as usize] = id;
                freelist.n_entries += 1;
                // added to freelist, so we can free it in the file
                clear_page(self.page(id).unwrap() as usize);
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

        assert_eq!(mapping.header().size, 2);
        assert_eq!(mapping.alloc(), 1);
        assert_eq!(mapping.header().size, 2);
        assert_eq!(mapping.alloc(), 2);
        assert_eq!(mapping.header().size, 4);
        assert_eq!(mapping.alloc(), 3);
        assert_eq!(mapping.header().size, 4);
        mapping.free(1);
        assert_eq!(mapping.alloc(), 1);
        mapping.free(1);
        mapping.free(2);
        mapping.free(3);
        mapping.alloc();
        mapping.alloc();
        mapping.alloc();
        assert_eq!(mapping.header().size, 4);
        assert_eq!(mapping.alloc(), 4);
        assert_eq!(mapping.header().size, 8);
    }
}
