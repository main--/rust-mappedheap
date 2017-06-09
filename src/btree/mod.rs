use super::ExtensibleMapping;
use extensiblemapping::PageId;
use futex::RwLock;
use std::fs::File;
use std::mem;

mod node;
mod ugly_hack;
use self::node::Node;
use self::ugly_hack::*;
use self::BTreePageInner::*;

pub struct MappedBTree {
    mapping: ExtensibleMapping
}

const ROOT_PAGE: PageId = 1;
type BTreePage = RwLock<BTreePageInner>;

#[repr(u16)]
enum BTreePageInner {
    #[allow(dead_code)] // compiler doesnt know shit actually
    Leaf(LeafNode),
    Inner(InnerNode),
}

impl MappedBTree {
    pub fn open(file: File) -> MappedBTree {
        MappedBTree {
            mapping: ExtensibleMapping::open(file)
        }
    }

    fn page(&self, id: PageId) -> Option<&BTreePage> {
        unsafe { self.mapping.page_mut(id).map(|x| &*x) }
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        let mut current = ROOT_PAGE;
        let mut _prev; // always need to keep previous page locked to avoid dragons
        loop {
            let lock = self.page(current).unwrap().read();
            match *lock {
                Inner(ref i) => current = i.traverse(key),
                Leaf(ref l) => return l.get(key),
            }
            _prev = lock;
        }
    }

    #[cfg(test)]
    fn debug_print(&self, id: PageId) {
        let lock = self.page(id).unwrap().read();
        match *lock {
            Inner(ref i) => i.debug(),
            Leaf(ref l) => l.debug(),
        }
    }

    pub fn try_insert(&self, key: u64, val: u64) -> bool {
        fn is_full(page: &BTreePageInner) -> bool {
            match page {
                &Inner(ref i) => i.full(),
                &Leaf(ref l) => l.full(),
            }
        }
        
        let mut path = Vec::new();
        let mut current = ROOT_PAGE;
        let mut go = true;
        while go {
            let lock = self.page(current).unwrap().read();
            let previd = current;
            match *lock {
                Inner(ref i) => current = i.traverse(key),
                Leaf(_) => go = false,
            }
            path.push((previd, lock));
        }

        let mut i_first_nonfull;
        let mut split_root = false;
        let parent;
        loop {
            i_first_nonfull = path.iter().rposition(|x| !is_full(&*x.1))
                .unwrap_or_else(|| { split_root = true; 0 });
            let first_nonfull = path.remove(i_first_nonfull).0; // release read lock

            // jaro fix 1
            path.truncate(i_first_nonfull);
            
            let write = self.page(first_nonfull).unwrap().write();
            if split_root || !is_full(&*write) {
                parent = (write, first_nonfull);
                break;
            }
        }

        // finally found a parent we (probably) don't have to split
        // only keep read locks from there to root as well as this write lock
        //path.truncate(i_first_nonfull);
        // now writelock our path to the leaf
        let mut wpath = Vec::new();
        let (mut current, mut current_id) = parent;
        loop {
            let next_id = match *current {
                Inner(ref i) => i.traverse(key),
                Leaf(_) => break,
            };
            let next = self.page(next_id).unwrap().write();
            wpath.push((mem::replace(&mut current, next), mem::replace(&mut current_id, next_id)));
        }

        // right now, current is the leaf
        // wpath contains writelocks at least up to the last one we have to touch
        // path contains readlocks above that
        wpath.push((current, current_id));

        // start by releasing writelocks that turned out to be unnecessary due to races
        if let Some(actual_first_nonfull) = wpath.iter().rposition(|x| !is_full(&*x.0)) {
            wpath.drain(..actual_first_nonfull);
        }

        let root_bonus = if split_root { 2 } else { 0 };
        // alloc new pages
        let mut newpages = Vec::new();
        for _ in 0..wpath.len() - 1 + root_bonus {
            if let Some(p) = self.mapping.try_alloc() {
                newpages.push(p);
            } else {
                // not enough memory available - free what we got and start over
                for p in newpages {
                    self.mapping.free(p);
                }
                return false;
            }
        }

        // run the split ops
        let mut key = key;
        let mut page_ref = None;
        for (j, ((mut old, _), &new)) in wpath.drain(1..).rev().zip(newpages.iter()).enumerate() {
            let mut newlock = self.page(new).unwrap().write();
            match *old {
                Inner(ref mut i) => {
                    assert_ne!(j, 0);
                    assert!(i.full());

                    *newlock = Inner(i.split(&mut key, page_ref.unwrap(), new).into());
                }
                Leaf(ref mut l) => {
                    assert_eq!(j, 0);
                    assert!(l.full());

                    *newlock = Leaf(l.split(&mut key, val, new).into());
                }
            }
            page_ref = Some(new);
        }

        // splits are done, register the last one or split root
        assert!(wpath.len() == 1);
        let (mut page, page_id) = wpath.remove(0);
        if split_root {
            assert_eq!(page_id, ROOT_PAGE);
            let newpagel_id = newpages[newpages.len() - 1];
            let newpager_id = newpages[newpages.len() - 2];
            let mut newpagel = self.page(newpagel_id).unwrap().write();
            let mut newpager = self.page(newpager_id).unwrap().write();
            *newpagel = mem::replace(&mut *page, unsafe { mem::zeroed() });
            match *newpagel {
                Inner(ref mut l) => {
                    assert!(l.full());

                    *newpager = Inner(l.split(&mut key, page_ref.unwrap(), newpager_id).into());
                }
                Leaf(ref mut l) => {
                    assert!(l.full());

                    *newpager = Leaf(l.split(&mut key, val, newpager_id).into());
                }
            }
            let mut tmp = InnerNodeActual::new(newpagel_id);
            tmp.insert(key, newpager_id);
            *page = Inner(tmp.into());
        } else {
            match *page {
                Inner(ref mut i) => i.insert(key, page_ref.unwrap()),
                Leaf(ref mut l) => l.insert(key, val),
            }
        }
        true
    }
}








#[cfg(test)]
mod tests {
    use super::*;
    use extensiblemapping::PAGESZ;
    use std::fs::OpenOptions;

    #[test]
    fn page_size() {
        assert_eq!(PAGESZ, mem::size_of::<BTreePage>());
    }

    #[test]
    fn alignment() {
        assert_eq!(1, mem::align_of::<InnerNode>());
    }

    #[test]
    fn size() {
        assert_eq!(mem::size_of::<InnerNode>(), mem::size_of::<InnerNodeActual>());
        assert_eq!(mem::size_of::<LeafNode>(), mem::size_of::<LeafNodeActual>());
    }

    #[test]
    fn it_works() {
        let mut file = OpenOptions::new().read(true).write(true).open("/tmp/btree.bin").unwrap();
        ExtensibleMapping::initialize(&mut file);
        let mut tree = MappedBTree::open(file);
        assert_eq!(tree.mapping.alloc(), 1);

        let mut prealloc = Vec::new();
        for i in 0..50 {
            prealloc.push(tree.mapping.alloc());
        }
        for i in prealloc {
            tree.mapping.free(i);
        }

        for i in 1..4096 {
            assert_eq!(tree.get(i), None, "{}", i);
            assert!(tree.try_insert(i, 1337 + i));
            assert_eq!(tree.get(i), Some(1337 + i));
        }

        if false
        {
            fn is_full(page: &BTreePageInner) -> bool {
                match page {
                    &Inner(ref i) => i.full(),
                    &Leaf(ref l) => l.full(),
                }
            }
            let lock = tree.page(2).unwrap().read();
            assert!(is_full(&*lock));
        }
    }
}
