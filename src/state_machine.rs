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
    C: RaftTypeConfig<Entry = openraft::Entry<C>, SnapshotData = Cursor<Vec<u8>>>,
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
    pub fn with_snapshot_dir(state: Arc<RwLock<S>>, snapshot_dir: PathBuf) -> io::Result<Self> {
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

fn snapshot_filename<C: RaftTypeConfig>(meta: &SnapshotMeta<C>) -> String
where
    LogId<C>: Serialize,
{
    let (term, index) = meta.last_log_id.as_ref().map_or((0, 0), |log_id| {
        // Extract term from CommittedLeaderId via serde, since it's an opaque
        // associated type for generic C. Supports both adv mode (object with
        // "term" field) and std mode (bare integer).
        let term = serde_json::to_value(&log_id.leader_id)
            .ok()
            .and_then(|v| match v {
                serde_json::Value::Number(n) => n.as_u64(),
                serde_json::Value::Object(m) => m.get("term").and_then(serde_json::Value::as_u64),
                _ => None,
            })
            .unwrap_or(0);
        (term, log_id.index)
    });
    format!("snap-{term}-{index}.json")
}

fn persist_snapshot<C, S>(dir: &Path, meta: &SnapshotMeta<C>, data: &[u8]) -> io::Result<()>
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

fn load_latest_snapshot<C, S>(dir: &Path) -> io::Result<Option<(SnapshotMeta<C>, S)>>
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
    C: RaftTypeConfig<Entry = openraft::Entry<C>, SnapshotData = Cursor<Vec<u8>>>,
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
        Strm: futures::Stream<Item = Result<openraft::storage::EntryResponder<C>, io::Error>>
            + Unpin
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

        self.last_applied.clone_from(&meta.last_log_id);
        self.last_membership.clone_from(&meta.last_membership);
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
                    self.last_applied.clone_from(&meta.last_log_id);
                    self.last_membership.clone_from(&meta.last_membership);
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
    C: RaftTypeConfig<Entry = openraft::Entry<C>, SnapshotData = Cursor<Vec<u8>>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_types::*;
    use openraft::storage::RaftStateMachine;
    use openraft::vote::RaftLeaderId;
    use openraft::vote::leader_id_adv::CommittedLeaderId;

    fn make_log_id(term: u64, node: u64, index: u64) -> LogId<TestTypeConfig> {
        LogId::new(CommittedLeaderId::new(term, node), index)
    }

    #[tokio::test]
    async fn new_state_machine_initial_state() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state);
        let (last_applied, membership) = sm.applied_state().await.unwrap();
        assert!(last_applied.is_none());
        assert!(membership.log_id().is_none());
    }

    #[tokio::test]
    async fn begin_receiving_snapshot_returns_empty_cursor() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state);
        let cursor = sm.begin_receiving_snapshot().await.unwrap();
        assert!(cursor.into_inner().is_empty());
    }

    #[tokio::test]
    async fn install_snapshot_updates_state() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state.clone());

        let new_state = TestState {
            data: [("k".into(), "v".into())].into(),
        };
        let snapshot_data = serde_json::to_vec(&new_state).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 1, 5)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-1".to_string(),
        };

        sm.install_snapshot(&meta, Cursor::new(snapshot_data))
            .await
            .unwrap();

        let s = state.read().await;
        assert_eq!(s.data.get("k").unwrap(), "v");

        let (last_applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(last_applied.unwrap().index, 5);
    }

    #[tokio::test]
    async fn get_current_snapshot_none_when_no_applied() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state);
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_none());
    }

    #[tokio::test]
    async fn get_current_snapshot_returns_data_after_install() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state);

        let new_state = TestState {
            data: [("x".into(), "y".into())].into(),
        };
        let snapshot_data = serde_json::to_vec(&new_state).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 1, 10)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-1".to_string(),
        };
        sm.install_snapshot(&meta, Cursor::new(snapshot_data))
            .await
            .unwrap();

        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_some());
        let snap = snap.unwrap();
        assert_eq!(snap.meta.last_log_id.as_ref().unwrap().index, 10);

        let loaded: TestState = serde_json::from_slice(&snap.snapshot.into_inner()).unwrap();
        assert_eq!(loaded.data.get("x").unwrap(), "y");
    }

    #[tokio::test]
    async fn get_snapshot_builder_and_build() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state);

        // Install some state first
        let new_state = TestState {
            data: [("a".into(), "b".into())].into(),
        };
        let snapshot_data = serde_json::to_vec(&new_state).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 1, 3)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-0".to_string(),
        };
        sm.install_snapshot(&meta, Cursor::new(snapshot_data))
            .await
            .unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id.as_ref().unwrap().index, 3);

        let loaded: TestState = serde_json::from_slice(&snap.snapshot.into_inner()).unwrap();
        assert_eq!(loaded.data.get("a").unwrap(), "b");
    }

    #[test]
    fn with_snapshot_dir_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let state = Arc::new(RwLock::new(TestState::default()));
        let _sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
            state,
            snap_dir.clone(),
        )
        .unwrap();
        assert!(snap_dir.exists());
    }

    #[tokio::test]
    async fn install_snapshot_persists_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
            state,
            snap_dir.clone(),
        )
        .unwrap();

        let new_state = TestState {
            data: [("disk".into(), "test".into())].into(),
        };
        let snapshot_data = serde_json::to_vec(&new_state).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 1, 7)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-1".to_string(),
        };
        sm.install_snapshot(&meta, Cursor::new(snapshot_data))
            .await
            .unwrap();

        // Verify files written
        assert!(snap_dir.join("current").exists());
        let current = std::fs::read_to_string(snap_dir.join("current")).unwrap();
        assert!(snap_dir.join(current.trim()).exists());
    }

    #[tokio::test]
    async fn build_snapshot_persists_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
            state,
            snap_dir.clone(),
        )
        .unwrap();

        // Install state so we have something to snapshot
        let new_state = TestState {
            data: [("build".into(), "snap".into())].into(),
        };
        let snapshot_data = serde_json::to_vec(&new_state).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 1, 2)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-0".to_string(),
        };
        sm.install_snapshot(&meta, Cursor::new(snapshot_data))
            .await
            .unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        let _snap = builder.build_snapshot().await.unwrap();

        assert!(snap_dir.join("current").exists());
    }

    #[tokio::test]
    async fn load_latest_snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
            state.clone(),
            snap_dir.clone(),
        )
        .unwrap();

        // Install a snapshot
        let new_state = TestState {
            data: [("load".into(), "test".into())].into(),
        };
        let snapshot_data = serde_json::to_vec(&new_state).unwrap();
        let meta = SnapshotMeta {
            last_log_id: Some(make_log_id(1, 1, 15)),
            last_membership: StoredMembership::default(),
            snapshot_id: "snap-1".to_string(),
        };
        sm.install_snapshot(&meta, Cursor::new(snapshot_data))
            .await
            .unwrap();

        // Create a new state machine from the same dir in a blocking context
        // (with_snapshot_dir uses blocking_write internally)
        let snap_dir_clone = snap_dir.clone();
        let fresh_state = tokio::task::spawn_blocking(move || {
            let fresh_state = Arc::new(RwLock::new(TestState::default()));
            let _sm2 = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
                fresh_state.clone(),
                snap_dir_clone,
            )
            .unwrap();
            fresh_state
        })
        .await
        .unwrap();

        let s = fresh_state.read().await;
        assert_eq!(s.data.get("load").unwrap(), "test");
    }

    #[tokio::test]
    async fn prune_old_snapshots_keeps_max() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let state = Arc::new(RwLock::new(TestState::default()));
        let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
            state,
            snap_dir.clone(),
        )
        .unwrap();

        // Install many snapshots to trigger pruning
        for i in 1..=6u64 {
            let new_state = TestState {
                data: [(format!("k{i}"), format!("v{i}"))].into(),
            };
            let snapshot_data = serde_json::to_vec(&new_state).unwrap();
            let meta = SnapshotMeta {
                last_log_id: Some(make_log_id(1, 1, i)),
                last_membership: StoredMembership::default(),
                snapshot_id: format!("snap-{i}"),
            };
            sm.install_snapshot(&meta, Cursor::new(snapshot_data))
                .await
                .unwrap();
        }

        // Count snap-*.json files
        let snap_count = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with("snap-")
                    && std::path::Path::new(&name)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            })
            .count();

        assert!(
            snap_count <= MAX_SNAPSHOTS,
            "Expected at most {MAX_SNAPSHOTS} snapshots, found {snap_count}"
        );
    }

    #[test]
    fn snapshot_filename_format() {
        let meta: SnapshotMeta<TestTypeConfig> = SnapshotMeta {
            last_log_id: Some(make_log_id(2, 1, 42)),
            last_membership: StoredMembership::default(),
            snapshot_id: "test".to_string(),
        };
        let name = snapshot_filename::<TestTypeConfig>(&meta);
        assert_eq!(name, "snap-2-42.json");
    }

    #[test]
    fn snapshot_filename_none_log_id() {
        let meta: SnapshotMeta<TestTypeConfig> = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "test".to_string(),
        };
        let name = snapshot_filename::<TestTypeConfig>(&meta);
        assert_eq!(name, "snap-0-0.json");
    }

    #[tokio::test]
    async fn get_current_snapshot_loads_from_disk_on_cold_start() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let state = Arc::new(RwLock::new(TestState::default()));

        // First: create SM, install snapshot, drop it
        {
            let mut sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
                state.clone(),
                snap_dir.clone(),
            )
            .unwrap();
            let new_state = TestState {
                data: [("cold".into(), "start".into())].into(),
            };
            let snapshot_data = serde_json::to_vec(&new_state).unwrap();
            let meta = SnapshotMeta {
                last_log_id: Some(make_log_id(1, 1, 20)),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-1".to_string(),
            };
            sm.install_snapshot(&meta, Cursor::new(snapshot_data))
                .await
                .unwrap();
        }

        // Second: create a fresh SM with no loaded state, call get_current_snapshot
        let fresh_state = Arc::new(RwLock::new(TestState::default()));
        let mut sm2 = HpcStateMachine::<TestTypeConfig, TestState>::new(fresh_state.clone());
        sm2.snapshot_dir = Some(snap_dir);

        let snap = sm2.get_current_snapshot().await.unwrap();
        assert!(snap.is_some());
        let snap = snap.unwrap();
        let loaded: TestState = serde_json::from_slice(&snap.snapshot.into_inner()).unwrap();
        assert_eq!(loaded.data.get("cold").unwrap(), "start");
    }

    #[tokio::test]
    async fn load_latest_snapshot_missing_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Create "current" pointing to a nonexistent file
        std::fs::write(snap_dir.join("current"), b"snap-0-999.json").unwrap();

        let result = load_latest_snapshot::<TestTypeConfig, TestState>(&snap_dir).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn state_accessor() {
        let state = Arc::new(RwLock::new(TestState::default()));
        let sm = HpcStateMachine::<TestTypeConfig, TestState>::new(state.clone());
        assert!(Arc::ptr_eq(&sm.state(), &state));
    }
}
