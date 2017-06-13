extern crate libc;
extern crate futex;
#[cfg(test)]
extern crate rand;

mod extensiblemapping;
mod btree;

pub use extensiblemapping::ExtensibleMapping;
pub use btree::MappedBTree;
