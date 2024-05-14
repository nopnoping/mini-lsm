#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.first_key.is_empty() {
            self.add_kv(&key, value);
            self.first_key.set_from_slice(key);
            return true;
        }

        let size = 8 + key.len() + value.len();
        if size + self.data.len() + self.offsets.len() > self.block_size {
            return false;
        }

        self.add_kv(&key, value);
        true
    }
    fn add_kv(&mut self, key: &KeySlice, value: &[u8]) {
        self.offsets.push(self.data.len() as u16);
        self.data
            .extend(Self::write_little_endian(key.len() as u16));
        self.data.extend(key.raw_ref());
        self.data
            .extend(Self::write_little_endian(value.len() as u16));
        self.data.extend(value);
    }
    pub fn write_little_endian(data: u16) -> [u8; 2] {
        let low = (data & 0xff) as u8;
        let high = ((data >> 8) & 0xff) as u8;
        [low, high]
    }
    pub fn read_little_endian(data: &[u8]) -> u16 {
        (data[0]) as u16 | ((data[1] as u16) << 8)
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data.clone(),
            offsets: self.offsets.clone(),
        }
    }
}
