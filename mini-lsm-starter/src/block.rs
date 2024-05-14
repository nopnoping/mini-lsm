#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);

        bytes.put(self.data.as_slice());
        self.offsets
            .iter()
            .for_each(|offset| bytes.put_u16(*offset));
        bytes.put_u16(self.offsets.len() as u16);

        bytes.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let n_offsets = (&data[len - 2..]).get_u16() as usize;
        let start_n_offsets = len - 2 - n_offsets * 2;

        let b_data = Vec::from(&data[0..start_n_offsets]);
        let mut b_offsets = Vec::with_capacity(n_offsets);
        for i in 0..n_offsets {
            let start = start_n_offsets + 2 * i;
            b_offsets.push((&data[start..start + 2]).get_u16());
        }

        Self {
            data: b_data,
            offsets: b_offsets,
        }
    }
}
