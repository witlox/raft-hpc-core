//! Backup export, verify, and restore for Raft state.
//!
//! Backup format: tar.gz containing:
//! ```text
//! backup-{timestamp}/
//!   metadata.json     — backup metadata (timestamp, term, index, app-specific)
//!   snapshot.json     — application state serialized as JSON
//! ```
//!
//! The backup functions are generic over the application state type `S` and
//! metadata type `M`, allowing each application to store its own metadata.

use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

use openraft::{RaftTypeConfig, StoredMembership};

use crate::BackupMetadataSource;

/// Core backup metadata, present in every backup regardless of application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata<M> {
    pub timestamp: DateTime<Utc>,
    pub snapshot_term: u64,
    pub snapshot_index: u64,
    /// Application-specific metadata (e.g., node count, entry count).
    pub app: M,
}

/// Export the current state to a tar.gz backup at the given path.
pub async fn export_backup<S>(
    state: &Arc<RwLock<S>>,
    path: &Path,
) -> io::Result<BackupMetadata<S::Metadata>>
where
    S: Serialize + Send + Sync + BackupMetadataSource,
{
    let state_guard = state.read().await;
    let state_json = serde_json::to_vec_pretty(&*state_guard).map_err(io::Error::other)?;
    let app_metadata = state_guard.backup_metadata();
    drop(state_guard);

    let metadata = BackupMetadata {
        timestamp: Utc::now(),
        snapshot_term: 0,
        snapshot_index: 0,
        app: app_metadata,
    };
    let metadata_json = serde_json::to_vec_pretty(&metadata).map_err(io::Error::other)?;

    let prefix = format!("backup-{}", metadata.timestamp.format("%Y%m%dT%H%M%SZ"));

    // Create tar.gz
    let file = std::fs::File::create(path)?;
    let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Add metadata.json
    let mut header = tar::Header::new_gnu();
    header.set_size(metadata_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(
        &mut header,
        format!("{prefix}/metadata.json"),
        metadata_json.as_slice(),
    )?;

    // Add snapshot.json
    let mut header = tar::Header::new_gnu();
    header.set_size(state_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(
        &mut header,
        format!("{prefix}/snapshot.json"),
        state_json.as_slice(),
    )?;

    tar.into_inner()?.finish()?;

    debug!("Exported backup to {}", path.display());
    Ok(metadata)
}

/// Verify a backup file's integrity and return its metadata.
pub fn verify_backup<S, M>(path: &Path) -> io::Result<BackupMetadata<M>>
where
    S: DeserializeOwned,
    M: DeserializeOwned + Serialize,
{
    let file = std::fs::File::open(path)?;
    let dec = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(dec);

    let mut found_metadata = false;
    let mut found_snapshot = false;
    let mut metadata: Option<BackupMetadata<M>> = None;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        match name.as_str() {
            "metadata.json" => {
                let mut buf = Vec::new();
                entry.read_to_end(&mut buf)?;
                metadata = Some(
                    serde_json::from_slice(&buf)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                );
                found_metadata = true;
            }
            "snapshot.json" => {
                let mut buf = Vec::new();
                entry.read_to_end(&mut buf)?;
                let _state: S = serde_json::from_slice(&buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                found_snapshot = true;
            }
            _ => {}
        }
    }

    if !found_metadata {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup missing metadata.json",
        ));
    }
    if !found_snapshot {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup missing snapshot.json",
        ));
    }

    Ok(metadata.unwrap())
}

/// Restore a backup into the given `data_dir` for Raft to load on restart.
///
/// Extracts the snapshot from the backup and places it in the snapshots
/// directory so the state machine will load it on next startup. The snapshot
/// is written in `PersistedSnapshot { meta, state }` format that
/// `load_latest_snapshot` expects.
pub fn restore_backup<C, S, M>(backup_path: &Path, data_dir: &Path) -> io::Result<BackupMetadata<M>>
where
    C: RaftTypeConfig,
    S: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned,
    StoredMembership<C>: Serialize + Default,
{
    // First verify the backup
    let metadata = verify_backup::<S, M>(backup_path)?;

    let snapshot_dir = data_dir.join("raft").join("snapshots");
    std::fs::create_dir_all(&snapshot_dir)?;

    // Extract the snapshot data
    let file = std::fs::File::open(backup_path)?;
    let dec = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(dec);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        if name == "snapshot.json" {
            let mut state_data = Vec::new();
            entry.read_to_end(&mut state_data)?;

            // Validate it's parseable
            let state: S = serde_json::from_slice(&state_data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let snap_filename = format!(
                "snap-{}-{}.json",
                metadata.snapshot_term, metadata.snapshot_index
            );
            let snap_path = snapshot_dir.join(&snap_filename);

            // Write in PersistedSnapshot { meta, state } format that
            // load_latest_snapshot expects.
            let persisted = serde_json::json!({
                "meta": {
                    "last_log_id": null,
                    "last_membership": StoredMembership::<C>::default(),
                    "snapshot_id": format!(
                        "restored-{}",
                        metadata.timestamp.format("%Y%m%dT%H%M%SZ")
                    ),
                },
                "state": state,
            });
            let json = serde_json::to_vec_pretty(&persisted).map_err(io::Error::other)?;
            std::fs::write(&snap_path, &json)?;

            // Update "current" pointer
            let current = snapshot_dir.join("current");
            std::fs::write(&current, snap_filename.as_bytes())?;

            debug!("Restored backup to {}", snap_path.display());
            break;
        }
    }

    // Clean up WAL since we're restoring from a snapshot
    let wal_dir = data_dir.join("raft").join("wal");
    if wal_dir.exists() {
        for entry in std::fs::read_dir(&wal_dir)? {
            let entry = entry?;
            let _ = std::fs::remove_file(entry.path());
        }
    }

    // Clean up vote/committed files
    let vote_path = data_dir.join("raft").join("vote.json");
    let committed_path = data_dir.join("raft").join("committed.json");
    let _ = std::fs::remove_file(&vote_path);
    let _ = std::fs::remove_file(&committed_path);

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_types::TestTypeConfig;

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestState {
        items: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestMetadata {
        item_count: usize,
    }

    impl crate::StateMachineState<TestTypeConfig> for TestState {
        fn apply(
            &mut self,
            _cmd: crate::test_types::TestCommand,
        ) -> crate::test_types::TestResponse {
            crate::test_types::TestResponse::Ok
        }

        fn blank_response() -> crate::test_types::TestResponse {
            crate::test_types::TestResponse::Ok
        }
    }

    impl BackupMetadataSource for TestState {
        type Metadata = TestMetadata;

        fn backup_metadata(&self) -> TestMetadata {
            TestMetadata {
                item_count: self.items.len(),
            }
        }
    }

    fn test_state() -> Arc<RwLock<TestState>> {
        Arc::new(RwLock::new(TestState {
            items: vec!["one".into(), "two".into(), "three".into()],
        }))
    }

    #[tokio::test]
    async fn export_and_verify_roundtrip() {
        let state = test_state();
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("test-backup.tar.gz");

        let export_meta = export_backup(&state, &backup_path).await.unwrap();
        assert_eq!(export_meta.app.item_count, 3);

        let verify_meta = verify_backup::<TestState, TestMetadata>(&backup_path).unwrap();
        assert_eq!(verify_meta.app.item_count, 3);
    }

    #[tokio::test]
    async fn verify_corrupt_backup_fails() {
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("corrupt.tar.gz");
        std::fs::write(&backup_path, b"not a valid tar.gz").unwrap();

        let result = verify_backup::<TestState, TestMetadata>(&backup_path);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn verify_missing_snapshot_fails() {
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("incomplete.tar.gz");

        // Create a tar.gz with only metadata
        let file = std::fs::File::create(&backup_path).unwrap();
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut tar_builder = tar::Builder::new(enc);

        let metadata = BackupMetadata {
            timestamp: Utc::now(),
            snapshot_term: 0,
            snapshot_index: 0,
            app: TestMetadata { item_count: 0 },
        };
        let metadata_json = serde_json::to_vec(&metadata).unwrap();

        let mut header = tar::Header::new_gnu();
        header.set_size(metadata_json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(
                &mut header,
                "backup/metadata.json",
                metadata_json.as_slice(),
            )
            .unwrap();

        tar_builder.into_inner().unwrap().finish().unwrap();

        let result = verify_backup::<TestState, TestMetadata>(&backup_path);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing snapshot.json")
        );
    }

    #[tokio::test]
    async fn restore_writes_snapshot_files() {
        let state = test_state();
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("backup.tar.gz");
        let data_dir = dir.path().join("restored");

        export_backup(&state, &backup_path).await.unwrap();
        let meta =
            restore_backup::<TestTypeConfig, TestState, TestMetadata>(&backup_path, &data_dir)
                .unwrap();

        assert_eq!(meta.app.item_count, 3);

        // Verify snapshot file was created
        let snap_dir = data_dir.join("raft").join("snapshots");
        assert!(snap_dir.exists());
        assert!(snap_dir.join("current").exists());

        // Verify the current pointer points to a valid file
        let current = std::fs::read_to_string(snap_dir.join("current")).unwrap();
        assert!(snap_dir.join(current.trim()).exists());
    }

    #[test]
    fn verify_nonexistent_backup_fails() {
        let result =
            verify_backup::<TestState, TestMetadata>(Path::new("/nonexistent/backup.tar.gz"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn restore_cleans_up_wal_and_vote() {
        let state = test_state();
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("backup.tar.gz");
        let data_dir = dir.path().join("restored");

        export_backup(&state, &backup_path).await.unwrap();

        // Create existing WAL and vote files to verify cleanup
        let wal_dir = data_dir.join("raft").join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::write(wal_dir.join("1.json"), b"old entry").unwrap();
        std::fs::write(wal_dir.join("2.json"), b"old entry").unwrap();

        let raft_dir = data_dir.join("raft");
        std::fs::write(raft_dir.join("vote.json"), b"old vote").unwrap();
        std::fs::write(raft_dir.join("committed.json"), b"old committed").unwrap();

        restore_backup::<TestTypeConfig, TestState, TestMetadata>(&backup_path, &data_dir).unwrap();

        // WAL entries should be cleaned up
        assert!(!wal_dir.join("1.json").exists());
        assert!(!wal_dir.join("2.json").exists());
        // vote and committed should be cleaned up
        assert!(!raft_dir.join("vote.json").exists());
        assert!(!raft_dir.join("committed.json").exists());
    }

    #[tokio::test]
    async fn verify_ignores_unknown_entries() {
        let state = test_state();
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("extra-files.tar.gz");

        // Create a tar.gz with metadata, snapshot, AND an extra unknown file
        let file = std::fs::File::create(&backup_path).unwrap();
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut tar_builder = tar::Builder::new(enc);

        let state_guard = state.read().await;
        let state_json = serde_json::to_vec_pretty(&*state_guard).unwrap();
        let app_metadata = state_guard.backup_metadata();
        drop(state_guard);

        let metadata = BackupMetadata {
            timestamp: Utc::now(),
            snapshot_term: 0,
            snapshot_index: 0,
            app: app_metadata,
        };
        let metadata_json = serde_json::to_vec(&metadata).unwrap();

        // metadata.json
        let mut header = tar::Header::new_gnu();
        header.set_size(metadata_json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(
                &mut header,
                "backup/metadata.json",
                metadata_json.as_slice(),
            )
            .unwrap();

        // unknown extra file
        let extra = b"some extra data";
        let mut header = tar::Header::new_gnu();
        header.set_size(extra.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "backup/extra-info.txt", extra.as_slice())
            .unwrap();

        // snapshot.json
        let mut header = tar::Header::new_gnu();
        header.set_size(state_json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "backup/snapshot.json", state_json.as_slice())
            .unwrap();

        tar_builder.into_inner().unwrap().finish().unwrap();

        // verify should succeed (extra file is ignored)
        let result = verify_backup::<TestState, TestMetadata>(&backup_path).unwrap();
        assert_eq!(result.app.item_count, 3);
    }

    #[tokio::test]
    async fn restore_snapshot_loadable_by_state_machine() {
        use crate::state_machine::HpcStateMachine;

        let state = test_state();
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("backup.tar.gz");
        let data_dir = dir.path().join("restored");

        export_backup(&state, &backup_path).await.unwrap();
        restore_backup::<TestTypeConfig, TestState, TestMetadata>(&backup_path, &data_dir).unwrap();

        // The state machine should be able to load the restored snapshot
        let snap_dir = data_dir.join("raft").join("snapshots");
        let fresh_state = tokio::task::spawn_blocking(move || {
            let fresh_state = Arc::new(tokio::sync::RwLock::new(TestState { items: vec![] }));
            let _sm = HpcStateMachine::<TestTypeConfig, TestState>::with_snapshot_dir(
                fresh_state.clone(),
                snap_dir,
            )
            .unwrap();
            fresh_state
        })
        .await
        .unwrap();

        let s = fresh_state.read().await;
        assert_eq!(s.items.len(), 3);
        assert_eq!(s.items[0], "one");
    }

    #[tokio::test]
    async fn verify_missing_metadata_fails() {
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join("no-metadata.tar.gz");

        // Create tar.gz with only snapshot
        let file = std::fs::File::create(&backup_path).unwrap();
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut tar_builder = tar::Builder::new(enc);

        let state = TestState::default();
        let snapshot_json = serde_json::to_vec(&state).unwrap();

        let mut header = tar::Header::new_gnu();
        header.set_size(snapshot_json.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(
                &mut header,
                "backup/snapshot.json",
                snapshot_json.as_slice(),
            )
            .unwrap();

        tar_builder.into_inner().unwrap().finish().unwrap();

        let result = verify_backup::<TestState, TestMetadata>(&backup_path);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing metadata.json")
        );
    }
}
