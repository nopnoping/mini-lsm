#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }

        while {
            let state = self.inner.state.read();
            !state.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }

        // recover
        let manifest;
        let manifest_path = path.join("MANIFEST");
        let mut next_sst_id = 1;
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(&path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();
            // recover l0-lmax
            for r in records {
                match r {
                    ManifestRecord::Flush(id) => {
                        memtables.remove(&id);
                        state.l0_sstables.insert(0, id);
                        next_sst_id = next_sst_id.max(id);
                    }
                    ManifestRecord::NewMemtable(id) => {
                        next_sst_id = next_sst_id.max(id);
                        memtables.insert(id);
                    }
                    ManifestRecord::Compaction(task, ids) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &ids);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(ids.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // recover sst
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let sst = SsTable::open(
                    *table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, *table_id))?,
                )?;
                state.sstables.insert(*table_id, Arc::new(sst));
            }
            next_sst_id += 1;

            // recover memtables
            if options.enable_wal {
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        // mem
        let mut r = snapshot.memtable.get(_key);
        if r.is_none() {
            for his in &snapshot.imm_memtables {
                r = his.get(_key);
                if r.is_some() {
                    break;
                }
            }
        }

        // l0 sst
        if r.is_none() {
            let h = farmhash::fingerprint32(_key);
            for sst_idx in &snapshot.l0_sstables {
                let sst = snapshot.sstables.get(sst_idx).unwrap().clone();
                if sst.bloom.is_none() || sst.bloom.as_ref().unwrap().may_contain(h) {
                    let itr =
                        SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(_key))
                            .unwrap();
                    if itr.key().raw_ref().cmp(_key).is_eq() {
                        r = Some(Bytes::copy_from_slice(itr.value()));
                        break;
                    }
                }
            }
        }

        // level sst
        if r.is_none() {
            let h = farmhash::fingerprint32(_key);
            for (_, sst_idx) in &snapshot.levels {
                for id in sst_idx.iter() {
                    let sst = snapshot.sstables.get(id).unwrap().clone();
                    if sst.bloom.is_none() || sst.bloom.as_ref().unwrap().may_contain(h) {
                        let itr = SsTableIterator::create_and_seek_to_key(
                            sst,
                            KeySlice::from_slice(_key),
                        )
                        .unwrap();
                        if itr.key().raw_ref().cmp(_key).is_eq() {
                            r = Some(Bytes::copy_from_slice(itr.value()));
                        }
                    }
                }
                if r.is_some() {
                    break;
                }
            }
        }

        if let Some(b) = r {
            if !b.is_empty() {
                return Ok(Some(b));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        self.write_kv(_key, _value)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.write_kv(_key, &[])
    }

    fn write_kv(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let r: Result<()>;
        let app_size: usize;
        {
            let state_read = self.state.read();
            r = state_read.memtable.put(_key, _value);
            app_size = state_read.memtable.approximate_size();
        }

        if app_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let state_read = self.state.read();
            if state_read.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state_read);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        r
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let new_memtables = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(id, self.path_of_wal(id))?)
        } else {
            Arc::new(MemTable::create(self.next_sst_id()))
        };

        self.freeze_memtable_with_memtable(new_memtables)?;

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(_state_lock_observer, ManifestRecord::NewMemtable(id))?;
        self.sync_dir()?;

        Ok(())
    }
    fn freeze_memtable_with_memtable(&self, new_memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        let mut new_state = guard.as_ref().clone();
        new_state.imm_memtables.insert(0, guard.memtable.clone());
        new_state.memtable = new_memtable;
        *guard = Arc::new(new_state);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let flush_imm = {
            let state = self.state.read();
            state.imm_memtables.last().unwrap().clone()
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        flush_imm.flush(&mut sst_builder)?;
        let id = flush_imm.id();
        let sst = sst_builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;

        {
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();
            new_state.imm_memtables.pop().unwrap();
            new_state.l0_sstables.insert(0, id);
            new_state.sstables.insert(id, Arc::new(sst));
            *state = Arc::new(new_state);
        }

        // Manifest
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Flush(id))?;
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        // memtable
        let mut vec = Vec::new();
        vec.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for imm in &snapshot.imm_memtables {
            vec.push(Box::new(imm.scan(_lower, _upper)));
        }
        let mem_itr = MergeIterator::create(vec);

        // l0 sst
        let mut vec = Vec::new();
        for sst_idx in &snapshot.l0_sstables {
            let sst = snapshot.sstables.get(sst_idx).unwrap().clone();
            if let Some(itr) = SsTableIterator::scan(sst, _lower, _upper) {
                vec.push(Box::new(itr));
            }
        }
        let l0_itr = MergeIterator::create(vec);

        // level sst
        let mut level_itr = Vec::with_capacity(snapshot.levels.len());
        for (_, ids) in &snapshot.levels {
            let mut vec = Vec::new();
            for id in ids.iter() {
                let sst = snapshot.sstables.get(id).unwrap().clone();
                if sst.range_overlap(_lower, _upper) {
                    vec.push(sst);
                }
            }
            let l1_itr = match _lower {
                Bound::Included(b) => {
                    SstConcatIterator::create_and_seek_to_key(vec, KeySlice::from_slice(b)).unwrap()
                }
                Bound::Excluded(b) => {
                    let mut itr =
                        SstConcatIterator::create_and_seek_to_key(vec, KeySlice::from_slice(b))
                            .unwrap();
                    if itr.is_valid() && itr.key().raw_ref().cmp(b).is_eq() {
                        itr.next()?;
                    }
                    itr
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(vec).unwrap(),
            };
            level_itr.push(Box::new(l1_itr));
        }

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(mem_itr, l0_itr)?,
                MergeIterator::create(level_itr),
            )?,
        )?))
    }
}
