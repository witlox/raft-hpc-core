//! File-backed log store for Raft.
//!
//! Persists WAL entries as individual JSON files and the vote as a separate file.
//! Layout:
//! ```text
//! {data_dir}/raft/
//!   vote.json          — persisted vote
//!   committed.json     — last committed log id
//!   wal/
//!     {index}.json     — one file per log entry
//! ```

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogId, LogState, OptionalSend, RaftLogReader, RaftTypeConfig};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// File-backed log store with in-memory cache.
#[derive(Clone)]
pub struct FileLogStore<C: RaftTypeConfig> {
    inner: Arc<RwLock<FileLogStoreInner<C>>>,
}

struct FileLogStoreInner<C: RaftTypeConfig> {
    wal_dir: PathBuf,
    vote_path: PathBuf,
    committed_path: PathBuf,
    vote: Option<openraft::vote::Vote<C>>,
    log: BTreeMap<u64, openraft::Entry<C>>,
    committed: Option<LogId<C>>,
    last_purged: Option<LogId<C>>,
}

impl<C> FileLogStore<C>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
    >,
{
    /// Create a new file-backed log store.
    ///
    /// Creates the directory structure if it doesn't exist, then loads
    /// existing state from disk.
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        let raft_dir = data_dir.join("raft");
        let wal_dir = raft_dir.join("wal");
        let vote_path = raft_dir.join("vote.json");
        let committed_path = raft_dir.join("committed.json");

        std::fs::create_dir_all(&wal_dir)?;

        // Load existing vote
        let vote = if vote_path.exists() {
            let data = std::fs::read_to_string(&vote_path)?;
            match serde_json::from_str(&data) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("Failed to parse vote.json, starting fresh: {e}");
                    None
                }
            }
        } else {
            None
        };

        // Load committed log id
        let committed = if committed_path.exists() {
            let data = std::fs::read_to_string(&committed_path)?;
            match serde_json::from_str(&data) {
                Ok(c) => Some(c),
                Err(e) => {
                    warn!("Failed to parse committed.json, starting fresh: {e}");
                    None
                }
            }
        } else {
            None
        };

        // Scan WAL directory and load entries
        let (log, last_purged) = Self::load_wal(&wal_dir)?;

        debug!(
            "FileLogStore loaded: {} entries, vote={:?}, committed={:?}",
            log.len(),
            vote,
            committed
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(FileLogStoreInner {
                wal_dir,
                vote_path,
                committed_path,
                vote,
                log,
                committed,
                last_purged,
            })),
        })
    }

    /// Load all WAL entries from disk.
    fn load_wal(wal_dir: &Path) -> io::Result<(BTreeMap<u64, openraft::Entry<C>>, Option<LogId<C>>)> {
        let mut log = BTreeMap::new();
        let mut last_purged: Option<LogId<C>> = None;

        // Check for purged marker
        let purged_path = wal_dir.join("purged.json");
        if purged_path.exists() {
            let data = std::fs::read_to_string(&purged_path)?;
            match serde_json::from_str(&data) {
                Ok(p) => last_purged = Some(p),
                Err(e) => warn!("Failed to parse purged.json: {e}"),
            }
        }

        for entry_result in std::fs::read_dir(wal_dir)? {
            let entry = entry_result?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Skip non-JSON files and special files
            if !name_str.ends_with(".json") || name_str == "purged.json" {
                continue;
            }

            // Parse index from filename
            let index_str = name_str.trim_end_matches(".json");
            let index: u64 = match index_str.parse() {
                Ok(i) => i,
                Err(_) => continue,
            };

            let data = std::fs::read_to_string(entry.path())?;
            match serde_json::from_str::<openraft::Entry<C>>(&data) {
                Ok(log_entry) => {
                    log.insert(index, log_entry);
                }
                Err(e) => {
                    warn!("Failed to parse WAL entry {}: {e}", name_str);
                }
            }
        }

        Ok((log, last_purged))
    }
}

fn write_file_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    // fsync the file
    let file = std::fs::File::open(&tmp)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    // fsync the parent directory
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

// ── RaftLogReader ──────────────────────────────────────────

#[derive(Clone)]
pub struct FileLogReader<C: RaftTypeConfig> {
    inner: Arc<RwLock<FileLogStoreInner<C>>>,
}

impl<C> RaftLogReader<C> for FileLogReader<C>
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

impl<C> RaftLogStorage<C> for FileLogStore<C>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
    >,
    openraft::Entry<C>: Clone,
{
    type LogReader = FileLogReader<C>;

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let inner = self.inner.read().await;
        let last = inner.log.values().last().map(|e| e.log_id.clone());
        Ok(LogState {
            last_purged_log_id: inner.last_purged.clone(),
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        FileLogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &C::Vote) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        let data = serde_json::to_vec(vote).map_err(io::Error::other)?;
        write_file_atomic(&inner.vote_path, &data)?;
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
            let data = serde_json::to_vec(&entry).map_err(io::Error::other)?;
            let path = inner.wal_dir.join(format!("{index}.json"));
            write_file_atomic(&path, &data)?;
            inner.log.insert(index, entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId<C>>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        if let Some(id) = last_log_id {
            let keys_to_remove: Vec<u64> =
                inner.log.range((id.index + 1)..).map(|(k, _)| *k).collect();
            for k in keys_to_remove {
                inner.log.remove(&k);
                let path = inner.wal_dir.join(format!("{k}.json"));
                if path.exists() {
                    std::fs::remove_file(&path)?;
                }
            }
        } else {
            // Remove all entries
            for k in inner.log.keys() {
                let path = inner.wal_dir.join(format!("{k}.json"));
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                }
            }
            inner.log.clear();
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        let keys_to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys_to_remove {
            inner.log.remove(&k);
            let path = inner.wal_dir.join(format!("{k}.json"));
            if path.exists() {
                let _ = std::fs::remove_file(&path);
            }
        }
        // Persist the purge marker
        let purged_path = inner.wal_dir.join("purged.json");
        let data = serde_json::to_vec(&log_id).map_err(io::Error::other)?;

        inner.last_purged = Some(log_id);
        write_file_atomic(&purged_path, &data)?;

        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId<C>>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        let data = serde_json::to_vec(&committed).map_err(io::Error::other)?;
        write_file_atomic(&inner.committed_path, &data)?;
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
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
        let vote = openraft::vote::Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();

        let mut reader = store.get_log_reader().await;
        let read = reader.read_vote().await.unwrap();
        assert_eq!(read.unwrap(), vote);

        // Verify persistence: create a new store from the same dir
        let mut store2 = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
        let mut reader2 = store2.get_log_reader().await;
        let read2 = reader2.read_vote().await.unwrap();
        assert_eq!(read2.unwrap(), vote);
    }

    #[tokio::test]
    async fn vote_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        let vote = openraft::vote::Vote::new(3, 2);
        {
            let mut store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
            store.save_vote(&vote).await.unwrap();
        }

        // "Restart" by creating new store
        let mut store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
        let mut reader = store.get_log_reader().await;
        let read = reader.read_vote().await.unwrap();
        assert_eq!(read.unwrap(), vote);
    }

    #[tokio::test]
    async fn committed_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        use openraft::vote::leader_id_adv::CommittedLeaderId;
        use openraft::vote::RaftLeaderId;
        let log_id = LogId::new(CommittedLeaderId::new(1, 1), 42);
        {
            let mut store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
            store.save_committed(Some(log_id.clone())).await.unwrap();
        }

        let mut store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
        let read = store.read_committed().await.unwrap();
        assert_eq!(read.unwrap(), log_id);
    }

    #[tokio::test]
    async fn wal_directory_created() {
        let dir = tempfile::tempdir().unwrap();
        let _store = FileLogStore::<TestTypeConfig>::new(dir.path()).unwrap();
        assert!(dir.path().join("raft/wal").exists());
    }
}
