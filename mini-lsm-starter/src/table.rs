#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering;
use std::collections::Bound;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        buf.put_u32(block_meta.len() as u32);
        block_meta.iter().for_each(|meta| {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.raw_ref());
        })
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let n = buf.get_u32();
        let mut vec = Vec::new();
        for index in 0..n {
            let offset = buf.get_u32() as usize;
            let f_key_len = buf.get_u16();
            let f_key = KeyBytes::from_bytes(buf.copy_to_bytes(f_key_len as usize));
            let l_key_len = buf.get_u16();
            let l_key = KeyBytes::from_bytes(buf.copy_to_bytes(l_key_len as usize));
            vec.push(BlockMeta {
                offset,
                first_key: f_key,
                last_key: l_key,
            });
        }
        vec
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // bloom
        let len = file.size();
        let bloom_offset = file.read(len - 4, 4).unwrap().as_slice().get_u32() as u64;
        let bloom = Bloom::decode(
            file.read(bloom_offset, len - 4 - bloom_offset)
                .unwrap()
                .as_slice(),
        )?;

        // meta
        let meta_offset = file.read(bloom_offset - 4, 4).unwrap().as_slice().get_u32() as u64;
        let block_meta = BlockMeta::decode_block_meta(
            file.read(meta_offset, bloom_offset - 4 - meta_offset)
                .unwrap()
                .as_slice(),
        );

        Ok(SsTable {
            file,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start = self.block_meta[block_idx].offset as u64;
        let end = if block_idx < self.block_meta.len() - 1 {
            self.block_meta[block_idx + 1].offset as u64
        } else {
            self.block_meta_offset as u64
        };

        let block = Block::decode(self.file.read(start, end - start).unwrap().as_slice());
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match self.block_cache.as_ref() {
            None => self.read_block(block_idx),
            Some(cache) => {
                let r = cache.get(&(self.id, block_idx));
                if r.is_none() {
                    let block = self.read_block(block_idx).unwrap();
                    cache.insert((self.id, block_idx), block.clone());
                    return Ok(block);
                }
                Ok(r.unwrap())
            }
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut low = 0;
        let mut high = self.block_meta.len() - 1;
        while low < high {
            let mid = low + high + 1 >> 1;
            match self.block_meta[mid].first_key.as_key_slice().cmp(&key) {
                Ordering::Less => low = mid,
                Ordering::Equal => return mid,
                Ordering::Greater => high = mid - 1,
            }
        }

        if low < self.block_meta.len() - 1 && self.block_meta[low].last_key.as_key_slice().lt(&key)
        {
            low += 1;
        }
        low
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn range_overlap(&self, user_begin: Bound<&[u8]>, user_end: Bound<&[u8]>) -> bool {
        match user_end {
            Bound::Excluded(key) if key <= self.first_key.raw_ref() => {
                return false;
            }
            Bound::Included(key) if key < self.first_key.raw_ref() => {
                return false;
            }
            _ => {}
        }
        match user_begin {
            Bound::Excluded(key) if key >= self.last_key().raw_ref() => {
                return false;
            }
            Bound::Included(key) if key > self.last_key().raw_ref() => {
                return false;
            }
            _ => {}
        }
        true
    }
}
