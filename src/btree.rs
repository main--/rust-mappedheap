use super::ExtensibleMapping;
use extensiblemapping::PageId;
use futex::RwLock;
use std::{mem, ptr};
use std::fs::File;
use std::ops::{Deref, DerefMut};

pub struct MappedBTree {
    mapping: ExtensibleMapping
}

const ROOT_PAGE: PageId = 1;

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
                BTreePageInner::Inner(ref i) =>
                    current = i.children()[find_slot(i.keys(), key)],
                BTreePageInner::Leaf(ref l) =>
                    return l.keys.iter().position(|&x| x == key).map(|i| l.data[i]),
            }
            _prev = lock;
        }
    }

    #[cfg(test)]
    fn debug_print(&self, id: PageId) {
        let lock = self.page(id).unwrap().read();
        match *lock {
            BTreePageInner::Inner(ref i) =>
                i.debug(),
            BTreePageInner::Leaf(ref l) =>
                l.debug(),
        }
    }

    pub fn try_insert(&self, key: u64, val: u64) -> bool {
        fn is_full(page: &BTreePageInner) -> bool {
            match page {
                &BTreePageInner::Inner(ref i) => i.full(),
                &BTreePageInner::Leaf(ref l) => l.full(),
            }
        }
        
        let mut path = Vec::new();
        let mut current = ROOT_PAGE;
        let mut go = true;
        while go {
            let lock = self.page(current).unwrap().read();
            let previd = current;
            match *lock {
                BTreePageInner::Inner(ref i) =>
                    current = i.children()[find_slot(i.keys(), key)],
                BTreePageInner::Leaf(_) => go = false,
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
                BTreePageInner::Inner(ref i) => i.children()[find_slot(i.keys(), key)],
                BTreePageInner::Leaf(_) => break,
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
                BTreePageInner::Inner(ref mut i) => {
                    assert_ne!(j, 0);
                    assert!(i.full());

                    *newlock = BTreePageInner::Inner(unsafe { mem::zeroed() });
                    key = match *newlock {
                        BTreePageInner::Inner(ref mut inner) => i.split(key, page_ref.unwrap(), inner),
                        _ => { unreachable!(); }
                    }
                }
                BTreePageInner::Leaf(ref mut l) => {
                    assert_eq!(j, 0);
                    assert!(l.full());

                    *newlock = BTreePageInner::Leaf(unsafe { mem::zeroed() });
                    key = match *newlock {
                        BTreePageInner::Leaf(ref mut leaf) => l.split(key, val, leaf, new),
                        _ => { unreachable!(); }
                    };
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
                BTreePageInner::Inner(ref mut l) => {
                    assert!(l.full());

                    *newpager = BTreePageInner::Inner(unsafe { mem::zeroed() });
                    key = match *newpager {
                        BTreePageInner::Inner(ref mut r) => l.split(key, page_ref.unwrap(), r),
                        _ => { unreachable!(); }
                    }
                }
                BTreePageInner::Leaf(ref mut l) => {
                    assert!(l.full());

                    *newpager = BTreePageInner::Leaf(unsafe { mem::zeroed() });
                    key = match *newpager {
                        BTreePageInner::Leaf(ref mut r) => l.split(key, val, r, newpager_id),
                        _ => { unreachable!(); }
                    }
                }
            }
            *page = BTreePageInner::Inner(unsafe { mem::zeroed() });
            match *page {
                BTreePageInner::Inner(ref mut root) => {
                    root.count_ = 1;
                    root.keys[0] = key;
                    root.children[0] = newpagel_id;
                    root.children[1] = newpager_id;
                }
                _ => { unreachable!(); }
            }
        } else {
            match *page {
                BTreePageInner::Inner(ref mut i) => i.insert(key, page_ref.unwrap()),
                BTreePageInner::Leaf(ref mut l) => l.insert(key, val),
            }
        }
        true
    }
}

fn find_slot(keys: &[u64], key: u64) -> usize {
    match keys.binary_search(&key) {
        Ok(i) => i,
        Err(i) => i,
    }
}
    

type BTreePage = RwLock<BTreePageInner>;

// beware ugly hacks because there are no packed enums
struct InnerNode {
    // keys: [u64; 255],
    // children: [PageId; 256],
    _rustc_pls_trust_me_when_i_say_i_know_the_right_alignment: [u8; 2 + (255 + 256) * 8],
}

#[repr(packed)]
struct InnerNodeActual {
    count_: u16,
    keys: [u64; 255],
    children: [PageId; 256],
}

impl InnerNodeActual {
    #[cfg(test)]
    fn debug(&self) {
        println!("Leaf n={} {:?} {:?}", self.count(), self.keys(), self.children());
    }

    fn keys(&self) -> &[u64] {
        &self.keys[..self.count()]
    }

    fn children(&self) -> &[PageId] {
        &self.children[.. self.count() + 1]
    }
    
    fn count(&self) -> usize {
        self.count_ as usize
    }

    fn full(&self) -> bool {
        self.count() == 255
    }

    fn insert(&mut self, key: u64, newpage: PageId) {
        assert!(!self.full());
        
        let i = find_slot(self.keys(), key);
        unsafe {
            ptr::copy(&self.keys[i], &mut self.keys[i + 1], self.count() - i);
            ptr::copy(&self.children[i], &mut self.children[i + 1], self.count() - i);
        }
        self.keys[i] = key;
        self.children[i+1] = newpage;
        self.count_ += 1;
    }
    
    fn split(&mut self, newkey: u64, newval: PageId, target: &mut InnerNode) -> u64 {
        debug_assert!(self.full());

        let mut remain = self.count() / 2;
        let mut rest = self.count() - remain;

        let i = find_slot(self.keys(), newkey);

        let ret = self.keys[remain];
        if i > remain {
            // add to target
            let before = i - remain - 1;
            target.keys[..before].copy_from_slice(&self.keys[remain+1..i]);
            target.children[..before+1].copy_from_slice(&self.children[remain..i]);


            target.keys[before] = newkey;
            target.children[before+1] = newval;

            let after = before + 1;
            target.keys[after..rest].copy_from_slice(&self.keys()[i..]);
            target.children[after+1..rest+1].copy_from_slice(&self.children()[i..]);
        } else {
            // add to self
            rest -= 1;
            target.keys[..rest].copy_from_slice(&self.keys()[remain+1..]);
            target.children[..rest+1].copy_from_slice(&self.children()[remain..]);

            unsafe {
                ptr::copy(&self.keys[i], &mut self.keys[i + 1], remain - i);
                ptr::copy(&self.children[i], &mut self.children[i + 1], remain - i);
            }
            self.keys[i] = newkey;
            self.children[i] = newval;
            
            remain += 1;
        }
        
        self.count_ = remain as u16;
        target.count_ = rest as u16;

        ret
    }
}

impl Deref for InnerNode {
    type Target = InnerNodeActual;

    fn deref(&self) -> &InnerNodeActual {
        unsafe { mem::transmute(self) }
    }
}

impl DerefMut for InnerNode {
    fn deref_mut(&mut self) -> &mut InnerNodeActual {
        unsafe { mem::transmute(self) }
    }
}

struct LeafNode {
    // keys: [u64; 255],
    // children: [PageId; 256],
    _rustc_pls_trust_me_when_i_say_i_know_the_right_alignment: [u8; 2 + (255 + 256) * 8],
}

#[repr(packed)]
struct LeafNodeActual {
    count_: u16,
    keys: [u64; 255],
    data: [u64; 255],
    next: PageId,
}

impl LeafNodeActual {
    #[cfg(test)]
    fn debug(&self) {
        println!("Leaf n={} {:?} {:?} next={}", self.count(), self.keys(), self.data(), self.next);
    }

    
    fn keys(&self) -> &[u64] {
        &self.keys[..self.count()]
    }
    
    fn data(&self) -> &[u64] {
        &self.data[..self.count()]
    }
    
    fn count(&self) -> usize {
        self.count_ as usize
    }

    fn full(&self) -> bool {
        self.count() == 255
    }

    fn insert(&mut self, key: u64, val: u64) {
        assert!(!self.full());

        let i = find_slot(self.keys(), key);
        unsafe {
            ptr::copy(&self.keys[i], self.keys.as_mut_ptr().offset(i as isize + 1), self.count() - i);
            ptr::copy(&self.data[i], self.data.as_mut_ptr().offset(i as isize + 1), self.count() - i);
        }
        self.keys[i] = key;
        self.data[i] = val;
        self.count_ += 1;
    }

    fn split(&mut self, newkey: u64, newval: u64, target: &mut LeafNode, target_id: PageId) -> u64 {
        debug_assert!(self.full());

        let mut remain = self.count() / 2;
        let mut rest = self.count() - remain;

        let i = find_slot(self.keys(), newkey);

        target.next = self.next;
        self.next = target_id;
        
        if i > remain {
            // add to target
            rest += 1;

            let before = i - remain;
            target.keys[..before].copy_from_slice(&self.keys[remain..i]);
            target.data[..before].copy_from_slice(&self.data[remain..i]);

            target.keys[i - remain] = newkey;
            target.data[i - remain] = newval;

            let after = i - remain + 1;
            target.keys[after..rest].copy_from_slice(&self.keys()[i..]);
            target.data[after..rest].copy_from_slice(&self.data()[i..]);
        } else {
            // add to self
            target.keys[..rest].copy_from_slice(&self.keys()[remain..]);
            target.data[..rest].copy_from_slice(&self.data()[remain..]);

            unsafe {
                ptr::copy(&self.keys[i], &mut self.keys[i + 1], remain - i);
                ptr::copy(&self.data[i], &mut self.data[i + 1], remain - i);
            }
            self.keys[i] = newkey;
            self.data[i] = newval;
            
            remain += 1;
        }
        
        self.count_ = remain as u16;
        target.count_ = rest as u16;

        target.keys[0]
    }
}

impl Deref for LeafNode {
    type Target = LeafNodeActual;

    fn deref(&self) -> &LeafNodeActual {
        unsafe { mem::transmute(self) }
    }
}

impl DerefMut for LeafNode {
    fn deref_mut(&mut self) -> &mut LeafNodeActual {
        unsafe { mem::transmute(self) }
    }
}



#[repr(u16)]
enum BTreePageInner {
    Leaf(LeafNode),
    #[allow(unused)] // compiler doesnt know shit actually
    Inner(InnerNode),
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
                    &BTreePageInner::Inner(ref i) => i.full(),
                    &BTreePageInner::Leaf(ref l) => l.full(),
                }
            }
            let lock = tree.page(2).unwrap().read();
            assert!(is_full(&*lock));
        }
    }
}
