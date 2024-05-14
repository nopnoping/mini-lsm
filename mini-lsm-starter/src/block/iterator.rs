#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::{Block, BlockBuilder};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut itr = BlockIterator::new(block);
        itr.seek_to_first();
        itr
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut itr = BlockIterator::new(block);
        itr.seek_to_first();
        itr.seek_to_key(key);
        itr
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let key_len = BlockBuilder::read_little_endian(&self.block.data[0..2]) as usize;
        let key = KeyVec::from_vec(Vec::from(&self.block.data[2..2 + key_len]));
        let value_len =
            BlockBuilder::read_little_endian(&self.block.data[2 + key_len..4 + key_len]) as usize;

        self.first_key = key.clone();
        self.key = key;
        self.idx = 0;
        self.value_range = (4 + key_len, 4 + key_len + value_len);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx = self.idx + 1;
        if self.idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        let key_start = self.block.offsets[self.idx] as usize;
        let key_len =
            BlockBuilder::read_little_endian(&self.block.data[key_start..key_start + 2]) as usize;
        let key = KeyVec::from_vec(Vec::from(
            &self.block.data[key_start + 2..key_start + 2 + key_len],
        ));
        let value_start = key_start + 2 + key_len;
        let value_len =
            BlockBuilder::read_little_endian(&self.block.data[value_start..value_start + 2])
                as usize;

        self.key = key;
        self.value_range = (value_start + 2, value_start + 2 + value_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        for n in 0..self.block.offsets.len() {
            let key_start = self.block.offsets[n] as usize;
            let key_len =
                BlockBuilder::read_little_endian(&self.block.data[key_start..key_start + 2])
                    as usize;
            let t_key = KeyVec::from_vec(Vec::from(
                &self.block.data[key_start + 2..key_start + 2 + key_len],
            ));

            if t_key.as_key_slice() >= key {
                let value_start = key_start + 2 + key_len;
                let value_len = BlockBuilder::read_little_endian(
                    &self.block.data[value_start..value_start + 2],
                ) as usize;
                self.key = t_key;
                self.idx = n;
                self.value_range = (value_start + 2, value_start + 2 + value_len);
                return;
            }
        }

        self.key = KeyVec::new();
        self.value_range = (0, 0);
    }
}
