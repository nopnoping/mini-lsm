#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = _path.as_ref();
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();

        while buf_ptr.has_remaining() {
            let key_len = buf_ptr.get_u16() as usize;
            let key = Bytes::copy_from_slice(&buf_ptr[..key_len]);
            buf_ptr.advance(key_len);
            let value_len = buf_ptr.get_u16() as usize;
            let value = Bytes::copy_from_slice(&buf_ptr[..value_len]);
            buf_ptr.advance(value_len);
            _skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::with_capacity(_key.len() + _value.len() + 2);
        buf.put_u16(_key.len() as u16);
        buf.put_slice(_key);
        buf.put_u16(_value.len() as u16);
        buf.put_slice(_value);
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
