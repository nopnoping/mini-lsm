#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut binary_heap = BinaryHeap::new();

        if iters.is_empty() {
            return MergeIterator {
                iters: binary_heap,
                current: None,
            };
        }

        if iters.iter().all(|itr| !itr.is_valid()) {
            let mut iters = iters;
            return MergeIterator {
                iters: binary_heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (index, itr) in iters.into_iter().enumerate() {
            if itr.is_valid() {
                binary_heap.push(HeapWrapper(index, itr));
            }
        }
        let current = binary_heap.pop();
        MergeIterator {
            iters: binary_heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|h| h.1.is_valid())
            .unwrap_or_else(|| false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // drop same key
        while let Some(mut inner) = self.iters.peek_mut() {
            if current.1.key() == inner.1.key() {
                if let e @ Err(_) = inner.1.next() {
                    PeekMut::pop(inner);
                    return e;
                }

                if !inner.1.is_valid() {
                    PeekMut::pop(inner);
                }
            } else {
                break;
            }
        }
        current.1.next()?;

        if !current.1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }

        if let Some(mut inner) = self.iters.peek_mut() {
            if *inner > *current {
                std::mem::swap(&mut *inner, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let n = if self.current.is_some() { 1 } else { 0 };
        n + self.iters.len()
    }
}
