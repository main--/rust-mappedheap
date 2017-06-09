use std::mem;
use std::ops::{Deref, DerefMut};
pub use super::node::{InnerNode as InnerNodeActual, LeafNode as LeafNodeActual};

// no packed enums and no way to force lower alignment -> need ugly hacks

pub struct InnerNode {
    // keys: [u64; 255],
    // children: [PageId; 256],
    _rustc_pls_trust_me_when_i_say_i_know_the_right_alignment: [u8; 2 + (255 + 256) * 8],
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

impl From<InnerNodeActual> for InnerNode {
    fn from(cool: InnerNodeActual) -> InnerNode {
        unsafe { mem::transmute(cool) }
    }
}

pub struct LeafNode {
    _rustc_pls_trust_me_when_i_say_i_know_the_right_alignment: [u8; 2 + (255 + 256) * 8],
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

impl From<LeafNodeActual> for LeafNode {
    fn from(cool: LeafNodeActual) -> LeafNode {
        unsafe { mem::transmute(cool) }
    }
}
