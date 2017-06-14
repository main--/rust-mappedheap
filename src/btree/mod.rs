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
    pub fn initialize(mut file: File) -> MappedBTree {
        ExtensibleMapping::initialize(&mut file);
        let mapping = ExtensibleMapping::open(file);
        assert_eq!(mapping.alloc(), ROOT_PAGE);
        MappedBTree { mapping }
    }

    pub fn open(file: File) -> MappedBTree {
        let mapping = ExtensibleMapping::open(file);
        mapping.page(ROOT_PAGE).expect("Opened an incomplete btree! (Race?)");
        MappedBTree { mapping }
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
    #[allow(dead_code)]
    fn debug_print(&self, id: PageId) {
        let lock = self.page(id).unwrap().read();
        match *lock {
            Inner(ref i) => i.debug(),
            Leaf(ref l) => l.debug(),
        }
    }

    pub fn insert(&self, key: u64, val: u64) {
        let (mut wpath, split_root) = self.wlock_subtree(key, |x| !x.full());

        let root_bonus = if split_root { 2 } else { 0 };
        // alloc new pages
        let mut newpages = Vec::new();
        for _ in 0..wpath.len() - 1 + root_bonus {
            newpages.push(self.mapping.alloc());
        }

        // run the split ops
        let mut key = key;
        let mut page_ref = None;
        for ((mut old, _), &new) in wpath.drain(1..).rev().zip(newpages.iter()) {
            self.split_into(&mut key, val, page_ref, &mut *old, new);
            page_ref = Some(new);
        }

        // splits are done, register the last one or split root
        let (mut page, _) = wpath.pop().unwrap();
        if split_root {
            let newpagel_id = newpages[newpages.len() - 1];
            let newpager_id = newpages[newpages.len() - 2];

            // root page always has to contain the root node
            // so we juggle the old root node to a new page
            let mut newpagel = self.page(newpagel_id).unwrap().write();
            *newpagel = mem::replace(&mut *page, unsafe { mem::uninitialized() });
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
    }

    pub fn remove(&self, key: u64) -> Option<u64> {
        // FIXME: this is pessimistic - most of these locks are wasted when we can just
        //        borrow from siblings (avg case)
        let (wpath, hit_root) = self.wlock_subtree(key, |x| !x.half_full());

        // first check if the element even exists
        // bailing out later is kinda hard
        match *wpath.last().unwrap().0 {
            Inner(..) => unreachable!(),
            Leaf(ref l) => {
                if l.keys().binary_search(&key).is_err() {
                    return None;
                }
            }
        }

        let mut iter = wpath.into_iter().rev();
        let (mut parent, mut parent_id) = iter.next().unwrap();
        let mut last_parent_slot = None;

        let mut ret = None;
        loop {
            let mut page = parent;
            let page_id = parent_id;
            let nextparent = iter.next();
            let root_exception = hit_root && nextparent.is_none();
            if page.count() == 1 {
                // can only happen at root
                assert!(root_exception);

                // remove
                let child_id = match *page {
                    Inner(ref mut inner) => {
                        inner.remove_idx(last_parent_slot.unwrap());
                        assert!(inner.count() == 0);
                        // right now, root is an inner node with only one element
                        // -> our only child inherits the whole business
                        inner.content()[0]
                    }
                    Leaf(ref mut l) => return l.remove(key), // tree is now empty, everything correct
                };

                let mut child = self.page(child_id).unwrap().write();
                *page = mem::replace(&mut *child, unsafe { mem::uninitialized() });
                drop(child);
                drop(page);
                self.mapping.free(child_id);
                return ret;
            } else if page.half_full() && !root_exception {
                // todo iterate one less
                let nextparent = nextparent.unwrap();
                parent = nextparent.0;
                parent_id = nextparent.1;

                let parent = match *parent {
                    Inner(ref mut i) => i,
                    _ => unreachable!(),
                };
                let slot = parent.find_slot(key);

                let mut sibling = None;
                let mut sibling_id = None;
                let mut is_right = false;

                if let Some(&siblingl) = parent.content().get(slot.wrapping_sub(1)) {
                    sibling_id = Some(siblingl);
                    sibling = Some(self.page(siblingl).unwrap().write());
                }
                if sibling.as_ref().map(|x| x.half_full()).unwrap_or(true) {
                    if let Some(&siblingr) = parent.content().get(slot + 1) {
                        sibling_id = Some(siblingr);
                        sibling = Some(self.page(siblingr).unwrap().write());
                        is_right = true;
                    }
                }

                let mut sibling = match sibling {
                    Some(x) => x,
                    None => unreachable!(),
                };
                if !sibling.half_full() {
                    // can borrow
                    match (&mut *page, &mut *sibling) {
                        (&mut Inner(ref mut p), &mut Inner(ref mut s)) => {
                            p.remove_idx(last_parent_slot.unwrap());
                            p.borrow(&mut *parent, slot, s, is_right);
                        }

                        (&mut Leaf(ref mut p), &mut Leaf(ref mut s)) => {
                            ret = p.remove(key);
                            p.borrow(&mut *parent, slot, s, is_right);
                        }
                        _ => unreachable!(),
                    };
                    assert!(ret.is_some());
                    return ret;
                }

                // need to merge
                match (&mut *page, &mut *sibling) {
                    (&mut Inner(ref mut p), &mut Inner(ref mut s)) => {
                        p.remove_idx(last_parent_slot.unwrap());
                        if is_right {
                            p.merge(s, parent.keys()[slot]);
                        } else {
                            s.merge(p, parent.keys()[slot - 1]);
                        }
                    }

                    (&mut Leaf(ref mut p), &mut Leaf(ref mut s)) => {
                        ret = p.remove(key); // TODO return this
                        if ret.is_none() {
                            return None;
                        }

                        if is_right {
                            p.merge(s, parent.keys()[slot]);
                        } else {
                            s.merge(p, parent.keys()[slot - 1]);
                        }
                    }
                    _ => unreachable!(),
                }

                drop(sibling);
                drop(page);

                if is_right {
                    self.mapping.free(sibling_id.unwrap());
                } else {
                    self.mapping.free(page_id);
                }

                last_parent_slot = Some(slot);
            } else {
                // easy mode
                match *page {
                    Inner(ref mut i) => { i.remove_idx(last_parent_slot.unwrap()).1; }
                    Leaf(ref mut l) => ret = l.remove(key),
                };
                assert!(ret.is_some());
                return ret;
            }
        }
    }

    /// Descends to the node (readlocks) containing the given key, then
    /// back up until we manage to write-lock a node that satisfies pred,
    /// THEN back down from there.
    /// Finally, we re-check the predicate an drop some of the upper
    /// locks if it turns out we didn't need them after all.
    fn wlock_subtree<F: Fn(&BTreePageInner) -> bool>(&self, key: u64, pred: F)
        -> (Vec<(RwLockWriteGuard<BTreePageInner>, PageId)>, bool) {
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

        let mut hit_root;
        let parent;
        let parent_id;
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
                parent_id = first_match;
                break;
            }
        }

        // now wlock back down to the leaf
        let mut wpath = Vec::new();
        let mut current = parent;
        let mut current_id = parent_id;
        while let Some(next_id) = current.traverse(key) {
            let next = self.page(next_id).unwrap().write();
            wpath.push((mem::replace(&mut current, next), mem::replace(&mut current_id, next_id)));
        }

        // right now, current is the leaf
        // wpath contains writelocks at least up to the first match
        // path contains all readlocks right above that TODO: why are we even holding those
        wpath.push((current, current_id));

        // start by releasing writelocks that turned out to be unnecessary due to races
        if let Some(actual_first_match) = wpath.iter().rposition(|x| pred(&x.0)) {
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
    fn count(&self) -> usize {
        match self {
            &Inner(ref i) => i.count(),
            &Leaf(ref l) => l.count(),
        }
    }

    fn full(&self) -> bool {
        match self {
            &Inner(ref i) => i.full(),
            &Leaf(ref l) => l.full(),
        }
    }

    fn half_full(&self) -> bool {
        match self {
            &Inner(ref i) => i.half_full(),
            &Leaf(ref l) => l.half_full(),
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
    use rand::{Rng, XorShiftRng};

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
    fn it_works_small() {
        it_works(10000);
    }

    #[test]
    #[ignore]
    fn it_works_big() {
        it_works(600000);
    }

    fn it_works(size: u64) {
        let file = OpenOptions::new().read(true).write(true).create(true)
            .truncate(true).open("/dev/shm/btree.bin").unwrap();
        let tree = MappedBTree::initialize(file);

        let range = 0..size;
        let mut rng = XorShiftRng::new_unseeded();

        let mut values: Vec<_> = range.clone().collect();
        rng.shuffle(&mut values);

        for &i in &values {
            assert_eq!(tree.get(i), None);
            tree.insert(i, i);
            assert_eq!(tree.get(i), Some(i));
        }

        for &i in &values {
            assert_eq!(tree.get(i), Some(i));
        }

        rng.shuffle(&mut values);

        for &i in &values {
            assert_eq!(tree.get(i), Some(i));
            assert_eq!(tree.remove(i), Some(i));
            assert_eq!(tree.remove(i), None);
            assert_eq!(tree.get(i), None);
        }
    }
}
