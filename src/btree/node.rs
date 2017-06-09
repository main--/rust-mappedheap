use std::{ptr, mem};
use extensiblemapping::PageId;

pub trait Node<T> : Sized {
    #[cfg(test)]
    fn debug(&self);
    fn keys(&self) -> &[u64];
    fn content(&self) -> &[T];
    fn count(&self) -> usize;
    fn full(&self) -> bool;
    fn insert(&mut self, key: u64, data: T);
    fn split(&mut self, key: &mut u64, newval: T, target_id: PageId) -> Self;

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
        println!("Leaf n={} {:?} {:?}", self.count(), self.keys(), self.content());
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

    fn full(&self) -> bool {
        self.count() == 255
    }

    fn insert(&mut self, key: u64, newpage: PageId) {
        assert!(!self.full());
        
        let i = self.find_slot(key);
        unsafe {
            ptr::copy(&self.keys[i], &mut self.keys[i + 1], self.count() - i);
            ptr::copy(&self.children[i], &mut self.children[i + 1], self.count() - i);
        }
        self.keys[i] = key;
        self.children[i+1] = newpage;
        self.count_ += 1;
    }
    
    fn split(&mut self, key: &mut u64, newval: PageId, _: PageId) -> InnerNode {
        debug_assert!(self.full());

        let newkey = *key;
        let mut target: InnerNode = unsafe { mem::uninitialized() };

        let mut remain = self.count() / 2;
        let mut rest = self.count() - remain;

        let i = self.find_slot(newkey);

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
}



#[repr(packed)]
pub struct LeafNode {
    count_: u16,
    keys: [u64; 255],
    data: [u64; 255],
    next: PageId,
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

    fn full(&self) -> bool {
        self.count() == 255
    }

    fn insert(&mut self, key: u64, val: u64) {
        assert!(!self.full());

        let i = self.find_slot(key);
        unsafe {
            ptr::copy(&self.keys[i], self.keys.as_mut_ptr().offset(i as isize + 1), self.count() - i);
            ptr::copy(&self.data[i], self.data.as_mut_ptr().offset(i as isize + 1), self.count() - i);
        }
        self.keys[i] = key;
        self.data[i] = val;
        self.count_ += 1;
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
}

