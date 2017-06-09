use super::ExtensibleMapping;
use extensiblemapping::PageId;
use futex::{RwLock, RwLockWriteGuard};
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
        let (mut wpath, split_root) = self.wlock_subtree(key, |x| !x.full());

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
        for (mut old, &new) in wpath.drain(1..).rev().zip(newpages.iter()) {
            self.split_into(&mut key, val, page_ref, &mut *old, new);
            page_ref = Some(new);
        }

        // splits are done, register the last one or split root
        debug_assert!(wpath.len() == 1);
        let mut page = wpath.remove(0);
        if split_root {
            let newpagel_id = newpages[newpages.len() - 1];
            let newpager_id = newpages[newpages.len() - 2];

            // root page always has to contain the root node
            // so we juggle the old root node to a new page
            let mut newpagel = self.page(newpagel_id).unwrap().write();
            *newpagel = mem::replace(&mut *page, unsafe { mem::zeroed() });
            // from there, split it to the other new page
            self.split_into(&mut key, val, page_ref, &mut *newpagel, newpager_id);
            // and finally create a new root node from scratch
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

    /// Descends to the node (readlocks) containing the given key, then
    /// back up until we manage to write-lock a node that satisfies pred,
    /// THEN back down from there.
    /// Finally, we re-check the predicate an drop some of the upper
    /// locks if it turns out we didn't need them after all.
    fn wlock_subtree<F: Fn(&BTreePageInner) -> bool>(&self, key: u64, pred: F)
                                                     -> (Vec<RwLockWriteGuard<BTreePageInner>>, bool) {
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

        let mut hit_root = false;
        let parent;
        loop {
            let o_first_match = path.iter().rposition(|x| pred(&x.1));
            hit_root = o_first_match.is_none();
            let i_first_match = o_first_match.unwrap_or(0);


            // release read lock ...
            let first_match = path.swap_remove(i_first_match).0;
            // ... and all below this one
            // (may only ever lock downwards)
            path.truncate(i_first_match);

            let write = self.page(first_match).unwrap().write();
            if hit_root || pred(&*write) {
                parent = write;
                break;
            }
        }

        // now wlock back down to the leaf
        let mut wpath = Vec::new();
        let mut current = parent;
        while let Some(next_id) = current.traverse(key) {
            let next = self.page(next_id).unwrap().write();
            wpath.push(mem::replace(&mut current, next));
        }

        // right now, current is the leaf
        // wpath contains writelocks at least up to the first match
        // path contains all readlocks right above that TODO: why are we even holding those
        wpath.push(current);

        // start by releasing writelocks that turned out to be unnecessary due to races
        if let Some(actual_first_match) = wpath.iter().rposition(|x| pred(&*x)) {
            wpath.drain(..actual_first_match);

            // if we initially hit the root but now found out that we no longer do
            // => reset the flag
            hit_root = false;
        }

        (wpath, hit_root)
    }

    fn split_into(&self, key: &mut u64, val: u64, page_ref: Option<PageId>,
                  page: &mut BTreePageInner, target_id: PageId) {
        let mut target = self.page(target_id).unwrap().write();
        *target = match *page {
            Inner(ref mut l) => Inner(l.split(key, page_ref.unwrap(), target_id).into()),
            Leaf(ref mut l) => Leaf(l.split(key, val, target_id).into()),
        }
    }
}

#[repr(u16)]
enum BTreePageInner {
    #[allow(dead_code)] // compiler doesnt know shit actually
    Leaf(LeafNode),
    Inner(InnerNode),
}

impl BTreePageInner {
    fn full(&self) -> bool {
        match self {
            &Inner(ref i) => i.full(),
            &Leaf(ref l) => l.full(),
        }
    }

    fn traverse(&self, key: u64) -> Option<PageId> {
        match *self {
            Inner(ref i) => Some(i.traverse(key)),
            Leaf(_) => None,
        }
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
