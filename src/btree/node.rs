use std::{ptr, mem};
use extensiblemapping::PageId;

pub trait Node<T> : Sized {
    #[cfg(test)]
    fn debug(&self);
    fn keys(&self) -> &[u64];
    fn content(&self) -> &[T];
    fn count(&self) -> usize;
    fn half_full(&self) -> bool {
        self.count() == 127
    }
    fn full(&self) -> bool {
        self.count() == 255
    }
    fn insert(&mut self, key: u64, data: T) {
        let i = self.find_slot(key);
        self.insert_idx(i, key, data);
    }

    fn insert_idx(&mut self, i: usize, key: u64, data: T);

    fn remove(&mut self, key: u64) -> Option<T> {
        let i = self.find_slot(key);
        if self.keys()[i] == key {
            Some(self.remove_idx(i).1)
        } else {
            None
        }
    }

    fn remove_idx(&mut self, key: usize) -> (u64, T);
    fn split(&mut self, key: &mut u64, newval: T, target_id: PageId) -> Self;

    fn borrow(&mut self, parent: &mut InnerNode, parent_slot: usize, sibling: &mut Self, is_right: bool);
    fn merge(&mut self, sibling: &mut Self, parent_key: u64);

    fn find_slot(&self, key: u64) -> usize {
        match self.keys().binary_search(&key) {
            Ok(i) => i,
            Err(i) => i,
        }
    }
}

#[repr(packed)]
pub struct InnerNode {
    count_: u16,
    keys: [u64; 255],
    children: [PageId; 256],
}

impl InnerNode {
    pub fn new(init_prev: PageId) -> InnerNode {
        let mut node: InnerNode = unsafe { mem::uninitialized() };
        node.count_ = 0;
        node.children[0] = init_prev;
        node
    }

    pub fn traverse(&self, key: u64) -> PageId {
        self.content()[self.find_slot(key)]
    }
}

impl Node<PageId> for InnerNode {
    #[cfg(test)]
    fn debug(&self) {
        println!("Inner n={} {:?} {:?}", self.count(), self.keys(), self.content());
    }

    fn keys(&self) -> &[u64] {
        &self.keys[..self.count()]
    }

    fn content(&self) -> &[PageId] {
        &self.children[.. self.count() + 1]
    }

    fn count(&self) -> usize {
        self.count_ as usize
    }

    fn insert_idx(&mut self, i: usize, key: u64, newpage: PageId) {
        assert!(!self.full());

        unsafe {
            ptr::copy(&self.keys[i], &mut self.keys[i + 1], self.count() - i);
            ptr::copy(&self.children[i + 1], &mut self.children[i + 2], self.count() - i);
        }
        self.keys[i] = key;
        self.children[i + 1] = newpage;
        self.count_ += 1;
    }

    fn remove_idx(&mut self, i: usize) -> (u64, PageId) {
        // assert!(!self.half_full());
        // have to allow temporarily violation of tree invariants
        // as we have to remove before we can merge inner nodes

        let ret = (self.keys[i - 1], self.children[i]);

        unsafe {
            ptr::copy(&self.keys[i], &mut self.keys[i - 1], self.count() - i);
            ptr::copy(&self.children[i + 1], &mut self.children[i], self.count() - i);
        }
        self.count_ -= 1;

        ret
    }

    fn split(&mut self, key: &mut u64, newval: PageId, _: PageId) -> InnerNode {
        debug_assert!(self.full());

        let newkey = *key;
        let mut target: InnerNode = unsafe { mem::uninitialized() };

        let mut remain = self.count() / 2;
        let mut rest = self.count() - remain;

        let i = self.find_slot(newkey) - 1;
        assert_eq!(self.keys[i], newkey);

        *key = self.keys[remain];
        if i > remain {
            // add to target
            let before = i - remain - 1;
            target.keys[..before].copy_from_slice(&self.keys[remain+1..i]);
            target.children[..before+1].copy_from_slice(&self.children[remain..i]);


            target.keys[before] = newkey;
            target.children[before+1] = newval;

            let after = before + 1;
            target.keys[after..rest].copy_from_slice(&self.keys()[i..]);
            target.children[after+1..rest+1].copy_from_slice(&self.content()[i..]);
        } else {
            // add to self
            rest -= 1;
            target.keys[..rest].copy_from_slice(&self.keys()[remain+1..]);
            target.children[..rest+1].copy_from_slice(&self.content()[remain..]);

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

        target
    }

    fn borrow(&mut self, parent: &mut InnerNode, parent_slot: usize,
              sibling: &mut InnerNode, is_right: bool) {
        assert!(self.half_full());
        assert!(!sibling.half_full());

        let (i_del, i_ins) = if is_right {
            (0, self.count())
        } else {
            (sibling.count() - 1, 0)
        };

        let (mut k, mut v) = sibling.remove_idx(i_del);
        if is_right {
            mem::swap(&mut k, &mut parent.keys[parent_slot + 1]);
            mem::swap(&mut v, &mut sibling.children[0]);
        } else {
            mem::swap(&mut k, &mut parent.keys[parent_slot]);
            mem::swap(&mut v, &mut self.children[0]);
        }
        self.insert_idx(i_ins, k, v);
    }

    fn merge(&mut self, sibling: &mut InnerNode, parent_key: u64) {
        assert!(self.count() + sibling.count() + 1 <= self.keys.len());
        assert!(self.keys[0] < sibling.keys[0]);

        let count = self.count();
        self.keys[count+1..][..sibling.count()].copy_from_slice(sibling.keys());
        self.children[count+1..][..sibling.count()+1].copy_from_slice(sibling.content());
        self.keys[count] = parent_key;
        self.count_ += sibling.count_ + 1;
    }

    fn find_slot(&self, key: u64) -> usize {
        match self.keys().binary_search(&key) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }
}



#[repr(packed)]
pub struct LeafNode {
    count_: u16,
    keys: [u64; 255],
    data: [u64; 255],
    next: PageId,
}

impl LeafNode {
    pub fn get(&self, key: u64) -> Option<u64> {
        self.keys().binary_search(&key).ok().map(|i| self.data[i])
    }
}

impl Node<u64> for LeafNode {
    #[cfg(test)]
    fn debug(&self) {
        println!("Leaf n={} {:?} {:?} next={}", self.count(), self.keys(), self.content(), self.next);
    }


    fn keys(&self) -> &[u64] {
        &self.keys[..self.count()]
    }

    fn content(&self) -> &[u64] {
        &self.data[..self.count()]
    }

    fn count(&self) -> usize {
        self.count_ as usize
    }

    fn insert_idx(&mut self, i: usize, key: u64, val: u64) {
        assert!(!self.full());

        unsafe {
            ptr::copy(&self.keys[i], self.keys.as_mut_ptr().offset(i as isize + 1), self.count() - i);
            ptr::copy(&self.data[i], self.data.as_mut_ptr().offset(i as isize + 1), self.count() - i);
        }
        self.keys[i] = key;
        self.data[i] = val;
        self.count_ += 1;
    }

    fn remove_idx(&mut self, i: usize) -> (u64, u64) {
        // assert!(!self.half_full());

        let ret = (self.keys[i], self.data[i]);

        unsafe {
            ptr::copy(&self.keys[i + 1], &mut self.keys[i], self.count() - i - 1);
            ptr::copy(&self.data[i + 1], &mut self.data[i], self.count() - i - 1);
        }
        self.count_ -= 1;

        ret
    }

    fn split(&mut self, key: &mut u64, newval: u64, target_id: PageId) -> LeafNode {
        debug_assert!(self.full());

        let newkey = *key;
        let mut target: LeafNode = unsafe { mem::uninitialized() };

        let mut remain = self.count() / 2;
        let mut rest = self.count() - remain;

        let i = self.find_slot(newkey);

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
            target.data[after..rest].copy_from_slice(&self.content()[i..]);
        } else {
            // add to self
            target.keys[..rest].copy_from_slice(&self.keys()[remain..]);
            target.data[..rest].copy_from_slice(&self.content()[remain..]);

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

        *key = target.keys[0];
        target
    }

    fn borrow(&mut self, parent: &mut InnerNode, parent_slot: usize,
              sibling: &mut LeafNode, is_right: bool) {
        assert!(self.half_full());
        assert!(!sibling.half_full());

        let (i_del, i_ins) = if is_right {
            (0, self.count())
        } else {
            (sibling.count() - 1, 0)
        };

        let (k, v) = sibling.remove_idx(i_del);
        if is_right {
            parent.keys[parent_slot /*+ 1*/] = sibling.keys[0];
        } else {
            parent.keys[parent_slot - 1] = k;
        }
        self.insert_idx(i_ins, k, v);
    }

    fn merge(&mut self, sibling: &mut LeafNode, _parent_key: u64) {
        assert!(self.count() + sibling.count() <= self.keys.len());
        assert!(self.keys[0] < sibling.keys[0]);

        let count = self.count();
        self.keys[count..][..sibling.count()].copy_from_slice(sibling.keys());
        self.data[count..][..sibling.count()].copy_from_slice(sibling.content());
        self.count_ += sibling.count_;
        self.next = sibling.next;
    }
}
