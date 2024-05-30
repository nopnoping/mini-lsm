#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::Leveled(_) => {
                unimplemented!()
            }
            CompactionTask::Tiered(_) => {
                unimplemented!()
            }
            CompactionTask::Simple(_) => {
                unimplemented!()
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut sst = Vec::new();
                // snapshot
                let snapshot = {
                    let state = self.state.read();
                    state.sstables.clone()
                };

                // get sst itr
                l0_sstables.iter().for_each(|id| {
                    sst.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(
                            snapshot.get(id).unwrap().clone(),
                        )
                        .unwrap(),
                    ))
                });
                l1_sstables.iter().for_each(|id| {
                    sst.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(
                            snapshot.get(id).unwrap().clone(),
                        )
                        .unwrap(),
                    ))
                });

                // compact to new sst
                let mut r = Vec::new();
                let mut itr = MergeIterator::create(sst);
                let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                let mut dirty = false;

                while itr.is_valid() {
                    if !itr.value().is_empty() {
                        sst_builder.add(itr.key(), itr.value());
                        dirty = true;

                        if sst_builder.estimated_size() >= self.options.target_sst_size {
                            let id = self.next_sst_id();
                            r.push(Arc::new(sst_builder.build(
                                id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(id),
                            )?));
                            sst_builder = SsTableBuilder::new(self.options.block_size);
                            dirty = false;
                        }
                    }

                    itr.next()?;
                }

                if dirty {
                    let id = self.next_sst_id();
                    r.push(Arc::new(sst_builder.build(
                        id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(id),
                    )?));
                }

                Ok(r)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // l0 l1 sst
        let (l0, l1) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        // compact
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0.clone(),
            l1_sstables: l1.clone(),
        };
        let new_sst = self.compact(&task)?;

        {
            // update state
            let _lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();

            // store hash
            new_sst.iter().for_each(|sst| {
                new_state.sstables.insert(sst.sst_id(), sst.clone());
            });

            let new_sst = new_sst.iter().map(|sst| sst.sst_id()).collect();
            new_state.levels[0] = (1, new_sst);
            for _ in 0..l0.len() {
                new_state.l0_sstables.pop();
            }
            // remove hash
            l0.iter().for_each(|id| {
                new_state.sstables.remove(id);
            });
            l1.iter().for_each(|id| {
                new_state.sstables.remove(id);
            });

            *state = Arc::new(new_state);
        }

        // remove file
        l0.iter()
            .try_for_each(|id| std::fs::remove_file(self.path_of_sst(*id)))?;
        l1.iter()
            .try_for_each(|id| std::fs::remove_file(self.path_of_sst(*id)))?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let state = self.state.read();
        if state.imm_memtables.len() >= self.options.num_memtable_limit {
            drop(state);
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
