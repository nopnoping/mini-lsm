#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use std::cmp::Ordering;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    // a: A,
    // b: B,
    // Add fields as need
    current: u8,
    itr: (A, B),
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut r = Self {
            itr: (a, b),
            current: 0,
        };
        r.handle_current();
        Ok(r)
    }

    fn handle_current(&mut self) {
        if self.itr.0.is_valid() && !self.itr.1.is_valid() {
            self.current = 0;
            return;
        }

        if !self.itr.0.is_valid() && self.itr.1.is_valid() {
            self.current = 1;
            return;
        }

        if self.itr.0.is_valid() && self.itr.1.is_valid() {
            let r = self.itr.0.key().cmp(&self.itr.1.key());
            self.current = match r {
                Ordering::Less => 0,
                Ordering::Equal => {
                    self.itr.1.next().unwrap();
                    0
                }
                Ordering::Greater => 1,
            };
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.current == 0 {
            self.itr.0.key()
        } else {
            self.itr.1.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.current == 0 {
            self.itr.0.value()
        } else {
            self.itr.1.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.itr.0.is_valid() || self.itr.1.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.current == 0 {
            self.itr.0.next()?;
        } else {
            self.itr.1.next()?;
        }

        self.handle_current();

        Ok(())
    }
}
