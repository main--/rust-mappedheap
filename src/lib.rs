#![warn(missing_docs)]
//! This crate provides ´MappedHeap`, an extensible memory mapped file
//! that keeps track of used and free pages with a simple freelist allocator.
//!
//! For details, see the type's documentation.

extern crate libc;
extern crate futex;
extern crate tempfile;
#[cfg(test)]
extern crate rand;

use libc::{mmap, munmap, PROT_READ, PROT_WRITE, MAP_SHARED, c_int, off_t, c_void, MAP_FAILED};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::{mem, ptr, cmp, io};
use std::cell::Cell;
use std::usize;
use std::path::Path;

use futex::raw::Mutex;
use futex::RwLock;
use tempfile::NamedTempFileOptions;

fn do_mmap(fd: c_int, offset: off_t, length: usize, fixed_addr: Option<usize>) -> io::Result<usize> {
    let ret = unsafe {
        mmap(fixed_addr.map(|x| x as *mut c_void).unwrap_or(ptr::null_mut()),
             length,
             PROT_READ | PROT_WRITE,
             MAP_SHARED,
             fd, offset)
    };

    if ret == MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

/// The size of a page in bytes.
pub const PAGESZ: usize = 4096;
const MAGIC: &[u8; 16] = b"\x89MAPHEAP\r\n\x1a\n\n\n\n\n";

/// An extensible memory mapped file that keeps track of used and free pages
/// with a simple freelist allocator.
///
/// The file will grow whenever necessary. It will always doube in size to
/// make sure resizes are rare.
///
/// # Example
///
/// ```
/// use mappedheap::MappedHeap;
///
/// let mapping = MappedHeap::open("/tmp/test.bin").unwrap();
/// let page_id = mapping.alloc();
/// let page_ptr = mapping.page(page_id).unwrap();
/// // do someting with page_ptr ...
/// mapping.free(page_id);
/// ```
pub struct MappedHeap {
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
                           ((self.offset + size) as usize * PAGESZ) as i64,
                           additional as usize * PAGESZ,
                           Some(addr_desired)).expect("Error while trying to grow mapping");
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

impl MappedHeap {
    fn header(&self) -> &mut FileHeader {
        unsafe { &mut *self.header_ptr }
    }

    fn initialize<W: Write>(file: &mut W) {
        let header = FileHeader {
            magic: *MAGIC,
            size: 2,
            _pad0: [0; 48],
            resize_lock: Mutex::new(),
            _pad1: [0; 52],
            alloc_lock: Mutex::new(),
            freelist_id: 1,
            _pad2: [0; 48],
            _pad_end: [0; HEADER_PAD_END],
        };
        let header: [u8; PAGESZ] = unsafe { mem::transmute(header) };
        file.write_all(&header).unwrap();
        file.write_all(&[0u8; PAGESZ]).unwrap();
    }

    /// Opens a file as a MappedHeap.
    ///
    /// This will panic if the file is not a valid MappedHeap.
    pub fn open_file(file: File) -> io::Result<MappedHeap> {
        let len = file.metadata()?.len();
        assert!(len <= usize::MAX as u64);

        let size = len / (PAGESZ as u64); // round down to full pages
        assert!(size > 0);

        let addr = do_mmap(file.as_raw_fd(), 0, size as usize * PAGESZ, None)?;

        Ok(MappedHeap {
            file,
            header_ptr: addr as *mut _,
            fragments: RwLock::new(vec![Fragment { addr, offset: 0, size: Cell::new(size) }]),
        }.sanity_check())
    }

    /// Opens a file as a MappedHeap.
    ///
    /// This will atomically create and initialize the file if it doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<MappedHeap> {
        loop {
            match OpenOptions::new().read(true).write(true).open(path.as_ref()) {
                Ok(file) => return MappedHeap::open_file(file),
                Err(ref x) if x.kind() == io::ErrorKind::NotFound => {
                    let dir = path.as_ref().parent().unwrap();
                    let stem = path.as_ref().file_stem().and_then(|x| x.to_str()).unwrap();
                    let ext = path.as_ref().extension().and_then(|x| x.to_str()).unwrap();
                    let mut tmp = NamedTempFileOptions::new().prefix(stem)
                        .suffix(&format!(".{}", ext)).create_in(dir)?;
                    MappedHeap::initialize(&mut tmp);
                    // ignore the result of this
                    // either we just created it
                    // or it already existed
                    // either way, go loop and try to open
                    let _ = tmp.persist_noclobber(path.as_ref());
                }
                Err(e) => return Err(e),
            }
        }
    }

    // FIXME: remove this - instead check on open and error if necessary
    fn sanity_check(self) -> MappedHeap {
        assert_eq!(&self.header().magic, MAGIC);
        self
    }

    /// Retrieves a pointer to a given page by Id, if exists within the file.
    /// The mapping is *not* guaranteed to be contiguous, thus operating out of the
    /// bounds of the returned pointer is undefined behavior.
    ///
    /// *Security note*: This only guarantees that the returned pointer points to
    /// memory backed by the file (and not some random other location).
    ///
    /// Most importantly, it does not protect you from inconsistencies caused
    /// by misuse of this API or outside interference (someone else messing with
    /// the file), such as:
    ///
    /// * The page is not allocated (or was double-free'd) - it might even contain the freelist.
    /// * The page is in use concurrently - data races will occur.
    /// * The page was arbitrarily modified by another application.
    ///
    /// **By unsafely operating on the returned pointer, it is your sole responsibility
    /// to make sure that your code does not violate memory safety!**
    ///
    /// # Panics
    ///
    /// * If the mapping needs to be extended but the syscall fails.
    ///   Resource exhaustion (memory limits) is the only documented case where this can happen.
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

    /// Retrieves a reference to a given page by Id, if it exists within the file.
    ///
    /// *Security note*: This only guarantees that the returned reference points to
    /// memory backed by the file (and not some random other location).
    ///
    /// Most importantly, it does not protect you from inconsistencies caused
    /// by misues of this API or outside interference (someone else messing with
    /// the file), such as:
    ///
    /// * The page is not allocated (or was double-free'd) - it might even contain the freelist.
    /// * The page is in use concurrently - data races will occur.
    /// * The page was arbitrarily modified by another application.
    ///
    /// In fact, even if you implement locking (you should!) you are still forced to
    /// just blindly assume that no other application (that doesn't respect your locks)
    /// is concurrently modifying the file. Whenever this assumption is violated, your
    /// your code may invoke undefined behavior.
    ///
    /// **By unsafely calling this method, it is your sole responsibility
    /// to make sure that your code does not violate memory safety!**
    ///
    /// # Panics
    ///
    /// * If T is not exactly page-sized.
    /// * If the mapping needs to be extended but the syscall fails.
    ///   Resource exhaustion (memory limits) is the only documented case where this can happen.
    pub unsafe fn page_ref<T>(&self, id: PageId) -> Option<&T> {
        assert_eq!(PAGESZ, mem::size_of::<T>());
        self.page(id).map(|x| &*(x as *const T))
    }

    // internal convenience function - &mut T is UB in like 100% of all cases
    unsafe fn page_mut<T>(&self, id: PageId) -> Option<&mut T> {
        assert_eq!(PAGESZ, mem::size_of::<T>());
        self.page(id).map(|x| &mut *(x as *mut T))
    }

    fn double_file(&self) {
        let header = self.header();
        header.resize_lock.acquire();
        header.size *= 2;
        self.file.set_len(header.size * (PAGESZ as u64)).expect("Failed to double file size");
        header.resize_lock.release();
    }

    /// Allocates a new page and returns its Id.
    ///
    /// This may double the file's size (if necessary).
    ///
    /// *Security note*: Outside interference as well as bugs in your code (see `free` for details)
    /// may corrupt the freelist structure. In that case, while this function will not violate
    /// memory safety, its behavior is undefined otherwise.
    ///
    /// # Panics
    ///
    /// * If the mapping needs to be extended but the syscall fails.
    ///   Resource exhaustion (memory limits) is the only documented case where this can happen.
    /// * If the file has to be extended but the syscall fails.
    /// * May panic if the freelist structure is corrupt.
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

        // In debug builds, zero out pages before we return them.
        #[cfg(debug)]
        unsafe { ptr::write_bytes(self.page(ret).unwrap(), 0, 1) };

        ret
    }

    /// Frees a page.
    ///
    /// Even though neither the mapping nor the file size will ever shrink,
    /// the disk space associated with this page may be reclaimed on supported
    /// operating and file systems (right now, only Linux is supported, have a
    /// look at fallocate(2) for a list of file systems that support hole punching).
    ///
    /// *Security note*: This only checks that the given page exists - nothing else.
    ///
    /// Invoking this method on pages that were not previously returned by `alloc`
    /// ("double-free") will corrupt the freelist structure.
    /// Concurrent modification by other applications not using this API may have
    /// the same effect. In both cases, while this function will not violate
    /// memory safety, its behavior is undefined otherwise.
    ///
    /// # Panics
    ///
    /// * If the given page id is not valid.
    /// * May panic if the freelist structure is corrupt.
    pub fn free(&self, id: PageId) {
        assert!(id != NULL_PAGE);
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

/// References a page.
pub type PageId = u64;

/// The null page guaranteed to always be invalid.
///
/// Internally, the first page (id 0) is reserved for the file header,
/// so it is never valid in any public calls (never returned by `alloc`,
/// never accessible through `page` etc.).
pub const NULL_PAGE: PageId = 0;

const HEADER_PAD_END: usize = PAGESZ - 64 * 3;

#[repr(C)]
struct FileHeader {
    magic: [u8; 16],
    _pad0: [u8; 48],
    resize_lock: Mutex,
    size: PageId, // number of pages
    _pad1: [u8; 52],
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
    use std::fs;

    #[test]
    fn size() {
        assert_eq!(mem::size_of::<FileHeader>(), PAGESZ);
    }

    #[test]
    fn it_works() {
        let _ = fs::remove_file("/tmp/map.bin");
        let mapping = MappedHeap::open("/tmp/map.bin").unwrap();

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

        let _ = fs::remove_file("/tmp/map.bin");
    }

    #[test]
    fn it_doesnt_bug() {
        let _ = fs::remove_file("/tmp/map2.bin");
        let mapping = MappedHeap::open("/tmp/map2.bin").unwrap();

        let mut allocs = Vec::new();
        for _ in 0..128 {
            let alloc = mapping.alloc();
            assert!(!allocs.contains(&alloc));
            allocs.push(alloc);
        }

        for alloc in allocs.drain(..) {
            mapping.free(alloc);
        }

        for _ in 0..129 {
            let alloc = mapping.alloc();
            assert!(!allocs.contains(&alloc));
            allocs.push(alloc);
        }

        let _ = fs::remove_file("/tmp/map2.bin");
    }
}
