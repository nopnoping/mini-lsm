#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::Bound;
use std::ops::Bound::Unbounded;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice, mem_table};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    _lower: Bound<Bytes>,
    _high: Bound<Bytes>,
    valid: bool,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut itr = SsTableIterator {
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0).unwrap()),
            blk_idx: 0,
            table,
            _lower: Unbounded,
            _high: Unbounded,
            valid: true,
        };
        itr.seek_to_first()?;
        Ok(itr)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0).unwrap());
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut itr = SsTableIterator {
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0).unwrap()),
            blk_idx: 0,
            table,
            _lower: Unbounded,
            _high: Unbounded,
            valid: true,
        };
        itr.seek_to_key(key)?;
        Ok(itr)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let idx = self.table.find_block_idx(key.clone());
        self.blk_idx = idx;
        self.blk_iter =
            BlockIterator::create_and_seek_to_key(self.table.read_block_cached(idx).unwrap(), key);
        Ok(())
    }

    pub fn scan(table: Arc<SsTable>, _lower: Bound<&[u8]>, _high: Bound<&[u8]>) -> Option<Self> {
        if !table.range_overlap(_lower, _high) {
            return None;
        }

        let mut itr = SsTableIterator {
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0).unwrap()),
            blk_idx: 0,
            table,
            _lower: mem_table::map_bound(_lower),
            _high: mem_table::map_bound(_high),
            valid: true,
        };
        match _lower {
            Bound::Included(b) => itr.seek_to_key(KeySlice::from_slice(b)).unwrap(),
            Bound::Excluded(b) => {
                itr.seek_to_key(KeySlice::from_slice(b)).unwrap();
                if itr.key().raw_ref().cmp(b).is_eq() {
                    itr.next().unwrap();
                }
            }
            Unbounded => itr.seek_to_first().unwrap(),
        };
        itr.check_end();
        Some(itr)
    }

    pub fn check_end(&mut self) {
        if self.is_valid() {
            match &self._high {
                Bound::Included(b) => {
                    if self.key().raw_ref().cmp(b).is_gt() {
                        self.valid = false;
                    }
                }
                Bound::Excluded(b) => {
                    if self.key().raw_ref().cmp(b).is_ge() {
                        self.valid = false;
                    }
                }
                Unbounded => {}
            }
        }
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid() && self.valid
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() && self.blk_idx < self.table.num_of_blocks() - 1 {
            self.blk_idx += 1;
            self.blk_iter = BlockIterator::create_and_seek_to_first(
                self.table.read_block_cached(self.blk_idx).unwrap(),
            );
        }
        self.check_end();
        Ok(())
    }
}
