//! Generic Raft state machine with persistent snapshot support.
//!
//! The state machine is parameterized by:
//! - `C: RaftTypeConfig` — the openraft type configuration
//! - `S: StateMachineState<C>` — the application state type
//!
//! Snapshot management (build, install, persist, prune, load) is handled
//! generically. The application provides the `apply()` logic via the
//! `StateMachineState` trait.

use std::io;
use std::io::Cursor;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta,
    StoredMembership,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::StateMachineState;

/// Maximum number of old snapshot files to keep.
const MAX_SNAPSHOTS: usize = 3;

/// The generic Raft state machine wrapping an application state.
pub struct HpcStateMachine<C, S>
where
    C: RaftTypeConfig<SnapshotData = Cursor<Vec<u8>>>,
    S: StateMachineState<C>,
{
    state: Arc<RwLock<S>>,
    last_applied: Option<LogId<C>>,
    last_membership: StoredMembership<C>,
    snapshot_idx: u64,
    snapshot_dir: Option<PathBuf>,
    _phantom: PhantomData<C>,
}

impl<C, S> HpcStateMachine<C, S>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
    S: StateMachineState<C>,
    LogId<C>: Serialize + DeserializeOwned,
    StoredMembership<C>: Serialize + DeserializeOwned,
{
    pub fn new(state: Arc<RwLock<S>>) -> Self {
        Self {
            state,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            snapshot_dir: None,
            _phantom: PhantomData,
        }
    }

    /// Create a state machine with persistent snapshot directory.
    ///
    /// On startup, loads the latest snapshot from disk if available.
    pub fn with_snapshot_dir(
        state: Arc<RwLock<S>>,
        snapshot_dir: PathBuf,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(&snapshot_dir)?;

        let mut sm = Self {
            state,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            snapshot_dir: Some(snapshot_dir),
            _phantom: PhantomData,
        };

        // Try to load the latest snapshot from disk
        if let Some(ref dir) = sm.snapshot_dir {
            if let Some((meta, app_state)) = load_latest_snapshot::<C, S>(dir)? {
                debug!("Loaded snapshot from disk at {:?}", meta.last_log_id);
                let mut state = sm.state.blocking_write();
                *state = app_state;
                sm.last_applied = meta.last_log_id;
                sm.last_membership = meta.last_membership;
                sm.snapshot_idx += 1;
            }
        }

        Ok(sm)
    }

    /// Get a read handle to the application state (for queries).
    pub fn state(&self) -> Arc<RwLock<S>> {
        Arc::clone(&self.state)
    }
}

/// Persisted snapshot format (stored alongside the state).
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "S: Serialize",
    deserialize = "S: serde::de::DeserializeOwned"
))]
struct PersistedSnapshot<C: RaftTypeConfig, S> {
    meta: PersistedSnapshotMeta<C>,
    state: S,
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct PersistedSnapshotMeta<C: RaftTypeConfig> {
    last_log_id: Option<LogId<C>>,
    last_membership: StoredMembership<C>,
    snapshot_id: String,
}

fn snapshot_filename<C: RaftTypeConfig>(meta: &SnapshotMeta<C>) -> String {
    let index = meta.last_log_id.as_ref().map_or(0, |l| l.index);
    format!("snap-0-{index}.json")
}

fn persist_snapshot<C, S>(
    dir: &Path,
    meta: &SnapshotMeta<C>,
    data: &[u8],
) -> io::Result<()>
where
    C: RaftTypeConfig,
    S: DeserializeOwned + Serialize,
    LogId<C>: Serialize,
    StoredMembership<C>: Serialize + Clone,
{
    let filename = snapshot_filename::<C>(meta);
    let path = dir.join(&filename);

    // Parse the state to persist it in our format
    let app_state: S =
        serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let persisted = PersistedSnapshot::<C, S> {
        meta: PersistedSnapshotMeta {
            last_log_id: meta.last_log_id.clone(),
            last_membership: meta.last_membership.clone(),
            snapshot_id: meta.snapshot_id.clone(),
        },
        state: app_state,
    };

    let json = serde_json::to_vec_pretty(&persisted).map_err(io::Error::other)?;

    // Atomic write
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &json)?;
    if let Ok(f) = std::fs::File::open(&tmp) {
        let _ = f.sync_all();
    }
    std::fs::rename(&tmp, &path)?;

    // Update "current" pointer
    let current = dir.join("current");
    let _ = std::fs::remove_file(&current);
    std::fs::write(&current, filename.as_bytes())?;

    // Prune old snapshots
    prune_old_snapshots(dir)?;

    debug!("Persisted snapshot to {}", path.display());
    Ok(())
}

fn prune_old_snapshots(dir: &Path) -> io::Result<()> {
    let mut snaps: Vec<(PathBuf, u64)> = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("snap-") && name_str.ends_with(".json") {
            let parts: Vec<&str> = name_str
                .trim_start_matches("snap-")
                .trim_end_matches(".json")
                .split('-')
                .collect();
            if parts.len() == 2 {
                if let Ok(index) = parts[1].parse::<u64>() {
                    snaps.push((entry.path(), index));
                }
            }
        }
    }

    snaps.sort_by(|a, b| b.1.cmp(&a.1));

    for (path, _) in snaps.iter().skip(MAX_SNAPSHOTS) {
        debug!("Pruning old snapshot: {}", path.display());
        let _ = std::fs::remove_file(path);
    }

    Ok(())
}

fn load_latest_snapshot<C, S>(
    dir: &Path,
) -> io::Result<Option<(SnapshotMeta<C>, S)>>
where
    C: RaftTypeConfig,
    S: DeserializeOwned,
    LogId<C>: DeserializeOwned,
    StoredMembership<C>: DeserializeOwned,
{
    let current_path = dir.join("current");
    if !current_path.exists() {
        return Ok(None);
    }

    let filename = std::fs::read_to_string(&current_path)?.trim().to_string();
    let snap_path = dir.join(&filename);

    if !snap_path.exists() {
        warn!("Current snapshot file {} not found", snap_path.display());
        return Ok(None);
    }

    let data = std::fs::read_to_string(&snap_path)?;
    let persisted: PersistedSnapshot<C, S> =
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let meta = SnapshotMeta {
        last_log_id: persisted.meta.last_log_id,
        last_membership: persisted.meta.last_membership,
        snapshot_id: persisted.meta.snapshot_id,
    };

    Ok(Some((meta, persisted.state)))
}

impl<C, S> RaftStateMachine<C> for HpcStateMachine<C, S>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
    S: StateMachineState<C>,
    LogId<C>: Serialize + DeserializeOwned,
    StoredMembership<C>: Serialize + DeserializeOwned + Clone,
{
    type SnapshotBuilder = HpcSnapshotBuilder<C, S>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C>>, StoredMembership<C>), io::Error> {
        Ok((self.last_applied.clone(), self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<C>, io::Error>,
            > + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;

        let mut stream = entries;
        while let Some(item) = stream.next().await {
            let (entry, responder) = item?;

            self.last_applied = Some(entry.log_id.clone());

            let response = match entry.payload {
                EntryPayload::Blank => S::blank_response(),
                EntryPayload::Normal(cmd) => {
                    let mut state = self.state.write().await;
                    state.apply(cmd)
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(self.last_applied.clone(), mem);
                    S::blank_response()
                }
            };

            if let Some(r) = responder {
                r.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        HpcSnapshotBuilder {
            state: Arc::clone(&self.state),
            last_applied: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            snapshot_idx: self.snapshot_idx,
            snapshot_dir: self.snapshot_dir.clone(),
            _phantom: PhantomData,
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let new_state: S = serde_json::from_slice(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Persist snapshot to disk if configured
        if let Some(ref dir) = self.snapshot_dir {
            persist_snapshot::<C, S>(dir, meta, &data)?;
        }

        let mut state = self.state.write().await;
        *state = new_state;

        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();
        self.snapshot_idx += 1;

        debug!("Installed snapshot at {:?}", meta.last_log_id);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, io::Error> {
        // If we have a snapshot dir, try loading from disk first (cold start case)
        if self.last_applied.is_none() {
            if let Some(ref dir) = self.snapshot_dir {
                if let Some((meta, app_state)) = load_latest_snapshot::<C, S>(dir)? {
                    let data = serde_json::to_vec(&app_state).map_err(io::Error::other)?;
                    let mut state = self.state.write().await;
                    *state = app_state;
                    self.last_applied = meta.last_log_id.clone();
                    self.last_membership = meta.last_membership.clone();
                    self.snapshot_idx += 1;

                    return Ok(Some(Snapshot {
                        meta,
                        snapshot: Cursor::new(data),
                    }));
                }
            }
        }

        let state = self.state.read().await;
        let data = serde_json::to_vec(&*state).map_err(io::Error::other)?;

        if self.last_applied.is_none() {
            return Ok(None);
        }

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_applied.clone(),
                last_membership: self.last_membership.clone(),
                snapshot_id: format!("snap-{}", self.snapshot_idx),
            },
            snapshot: Cursor::new(data),
        };

        Ok(Some(snapshot))
    }
}

/// Builds snapshots from the current application state.
pub struct HpcSnapshotBuilder<C, S>
where
    C: RaftTypeConfig,
    S: StateMachineState<C>,
{
    state: Arc<RwLock<S>>,
    last_applied: Option<LogId<C>>,
    last_membership: StoredMembership<C>,
    snapshot_idx: u64,
    snapshot_dir: Option<PathBuf>,
    _phantom: PhantomData<C>,
}

impl<C, S> RaftSnapshotBuilder<C> for HpcSnapshotBuilder<C, S>
where
    C: RaftTypeConfig<
        Entry = openraft::Entry<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
    S: StateMachineState<C>,
    LogId<C>: Serialize + DeserializeOwned,
    StoredMembership<C>: Serialize + DeserializeOwned + Clone,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, io::Error> {
        let state = self.state.read().await;
        let data = serde_json::to_vec(&*state).map_err(io::Error::other)?;

        self.snapshot_idx += 1;

        let meta = SnapshotMeta {
            last_log_id: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snap-{}", self.snapshot_idx),
        };

        // Persist snapshot to disk if configured
        if let Some(ref dir) = self.snapshot_dir {
            persist_snapshot::<C, S>(dir, &meta, &data)?;
        }

        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(data),
        };

        debug!("Built snapshot at {:?}", self.last_applied);
        Ok(snapshot)
    }
}
