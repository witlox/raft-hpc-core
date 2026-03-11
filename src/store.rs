//! In-memory log storage for Raft.
//!
//! This provides both `RaftLogStorage` and `RaftLogReader` implementations
//! backed by a `Vec<Entry>` and a stored vote. Suitable for testing and
//! single-process deployments; production would add persistence.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogId, LogState, OptionalSend, RaftLogReader, RaftTypeConfig};
use tokio::sync::RwLock;

/// In-memory log store shared behind an `Arc<RwLock<_>>`.
#[derive(Clone)]
pub struct MemLogStore<C: RaftTypeConfig> {
    inner: Arc<RwLock<MemLogStoreInner<C>>>,
}

struct MemLogStoreInner<C: RaftTypeConfig> {
    vote: Option<openraft::vote::Vote<C>>,
    log: BTreeMap<u64, openraft::Entry<C>>,
    committed: Option<LogId<C>>,
    last_purged: Option<LogId<C>>,
}

impl<C: RaftTypeConfig> Default for MemLogStore<C> {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemLogStoreInner {
                vote: None,
                log: BTreeMap::new(),
                committed: None,
                last_purged: None,
            })),
        }
    }
}

impl<C: RaftTypeConfig> MemLogStore<C> {
    pub fn new() -> Self {
        Self::default()
    }
}

// ── RaftLogReader ──────────────────────────────────────────

/// A cloneable reader into the in-memory log store.
#[derive(Clone)]
pub struct MemLogReader<C: RaftTypeConfig> {
    inner: Arc<RwLock<MemLogStoreInner<C>>>,
}

impl<C> RaftLogReader<C> for MemLogReader<C>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
    >,
    openraft::Entry<C>: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        let inner = self.inner.read().await;
        let entries: Vec<C::Entry> = inner.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<C::Vote>, io::Error> {
        let inner = self.inner.read().await;
        Ok(inner.vote.clone())
    }
}

// ── RaftLogStorage ─────────────────────────────────────────

impl<C> RaftLogStorage<C> for MemLogStore<C>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
    >,
    openraft::Entry<C>: Clone,
{
    type LogReader = MemLogReader<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let inner = self.inner.read().await;
        let last = inner.log.values().last().map(|e| e.log_id.clone());
        Ok(LogState {
            last_purged_log_id: inner.last_purged.clone(),
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        MemLogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &C::Vote) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        inner.vote = Some(vote.clone());
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<C>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.write().await;
        for entry in entries {
            let index = entry.log_id.index;
            inner.log.insert(index, entry);
        }
        // In-memory store: data is immediately durable
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId<C>>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        match last_log_id {
            Some(id) => {
                // Keep entries up to and including id.index
                let keys_to_remove: Vec<u64> =
                    inner.log.range((id.index + 1)..).map(|(k, _)| *k).collect();
                for k in keys_to_remove {
                    inner.log.remove(&k);
                }
            }
            None => {
                inner.log.clear();
            }
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        let keys_to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys_to_remove {
            inner.log.remove(&k);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId<C>>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C>>, io::Error> {
        let inner = self.inner.read().await;
        Ok(inner.committed.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_types::TestTypeConfig;

    #[tokio::test]
    async fn initial_state_is_empty() {
        let mut store = MemLogStore::<TestTypeConfig>::new();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = MemLogStore::<TestTypeConfig>::new();
        let vote = openraft::vote::Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();

        let mut reader = store.get_log_reader().await;
        let read = reader.read_vote().await.unwrap();
        assert_eq!(read.unwrap(), vote);
    }

    // Note: append/truncate/purge are tested implicitly via the integration
    // tests since IOFlushed::new is pub(crate) in openraft 0.10.
}
