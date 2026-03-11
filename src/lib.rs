//! # raft-hpc-core
//!
//! Shared Raft consensus infrastructure for HPC systems. Extracted from
//! lattice-quorum with minimal parameterization — each application provides
//! its own `TypeConfig` via `openraft::declare_raft_types!`.
//!
//! ## What's generic (in this crate)
//!
//! - Log stores (in-memory, file-backed, polymorphic variant)
//! - gRPC transport (network factory, transport server)
//! - In-memory network (for testing)
//! - State machine (snapshot management, apply dispatch)
//! - Backup (export, verify, restore)
//!
//! ## What's application-specific (NOT in this crate)
//!
//! - `TypeConfig` declaration (`openraft::declare_raft_types!`)
//! - Command and `CommandResponse` enums
//! - Application state (`GlobalState`, `JournalState`, etc.)
//! - `StateMachineState::apply()` implementation
//! - Client trait implementations
//! - Factory functions (`create_quorum`, etc.)

#![allow(clippy::significant_drop_tightening)]

pub mod backup;
pub mod log_store_variant;
pub mod network;
pub mod persistent_store;
pub mod state_machine;
pub mod store;
pub mod transport;
pub mod transport_server;

/// Generated protobuf types for the Raft transport service.
pub mod proto {
    #[allow(clippy::all, clippy::pedantic, clippy::nursery)]
    mod inner {
        tonic::include_proto!("raft_hpc.v1");
    }
    pub use inner::*;
}

use std::fmt;

use openraft::RaftTypeConfig;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Application state managed by the Raft state machine.
///
/// Implement this trait for your application's state type (e.g., `GlobalState`,
/// `JournalState`). The state machine will call `apply()` for each committed
/// command and use serde for snapshot serialization.
pub trait StateMachineState<C: RaftTypeConfig>:
    Serialize + DeserializeOwned + Default + Send + Sync + 'static
{
    /// Apply a committed command to the state, returning a response.
    fn apply(&mut self, cmd: C::D) -> C::R;

    /// Response value for blank entries and membership changes.
    fn blank_response() -> C::R;
}

/// Application state that supports backup metadata extraction.
///
/// Implement this to enable backup export/verify/restore with
/// application-specific metadata (e.g., node count, entry count).
pub trait BackupMetadataSource {
    /// Application-specific backup metadata type.
    type Metadata: Serialize + DeserializeOwned + fmt::Debug + Clone;

    /// Extract metadata from the current state for backup records.
    fn backup_metadata(&self) -> Self::Metadata;
}

// Re-exports for convenience.
pub use backup::{BackupMetadata, export_backup, restore_backup, verify_backup};
pub use log_store_variant::{LogReaderVariant, LogStoreVariant};
pub use network::MemNetworkFactory;
pub use persistent_store::FileLogStore;
pub use state_machine::HpcStateMachine;
pub use store::MemLogStore;
pub use transport::{GrpcNetworkFactory, PeerTlsConfig};
pub use transport_server::RaftTransportServer;

/// Test types for unit tests across all modules.
///
/// Provides a minimal `TestTypeConfig` with simple Command/Response enums
/// that satisfy all openraft requirements.
#[cfg(test)]
pub(crate) mod test_types {
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use std::io::Cursor;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub enum TestCommand {
        Set(String, String),
    }

    impl fmt::Display for TestCommand {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Set(k, v) => write!(f, "Set({k}, {v})"),
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub enum TestResponse {
        Ok,
    }

    impl fmt::Display for TestResponse {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Ok")
        }
    }

    openraft::declare_raft_types!(
        pub TestTypeConfig:
            D = TestCommand,
            R = TestResponse,
            NodeId = u64,
            Node = openraft::impls::BasicNode,
            SnapshotData = Cursor<Vec<u8>>,
    );

    /// Simple key-value state for integration tests.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct TestState {
        pub data: std::collections::HashMap<String, String>,
    }

    impl crate::StateMachineState<TestTypeConfig> for TestState {
        fn apply(&mut self, cmd: TestCommand) -> TestResponse {
            match cmd {
                TestCommand::Set(k, v) => {
                    self.data.insert(k, v);
                    TestResponse::Ok
                }
            }
        }

        fn blank_response() -> TestResponse {
            TestResponse::Ok
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use test_types::*;
    use tokio::sync::RwLock;

    /// Create a single-node in-memory quorum for testing.
    async fn create_test_quorum() -> (openraft::Raft<TestTypeConfig>, Arc<RwLock<TestState>>) {
        let state = Arc::new(RwLock::new(TestState::default()));
        let config = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );

        let log_store = MemLogStore::new();
        let sm = HpcStateMachine::new(Arc::clone(&state));
        let network = MemNetworkFactory::new();

        let raft = openraft::Raft::new(1, config, network, log_store, sm)
            .await
            .unwrap();

        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::new("127.0.0.1:0"));
        raft.initialize(members).await.unwrap();

        raft.wait(None)
            .metrics(|m| m.current_leader == Some(1), "leader elected")
            .await
            .unwrap();

        (raft, state)
    }

    /// Create a multi-node in-memory cluster for testing.
    async fn create_test_cluster(
        node_count: u64,
    ) -> Vec<(openraft::Raft<TestTypeConfig>, Arc<RwLock<TestState>>)> {
        let network_factory = MemNetworkFactory::new();
        let mut nodes = Vec::new();
        let mut members = BTreeMap::new();

        for id in 1..=node_count {
            members.insert(
                id,
                openraft::impls::BasicNode::new(format!("127.0.0.1:{}", 5000 + id)),
            );
        }

        for id in 1..=node_count {
            let state = Arc::new(RwLock::new(TestState::default()));
            let config = Arc::new(
                openraft::Config {
                    heartbeat_interval: 200,
                    election_timeout_min: 500,
                    election_timeout_max: 1000,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            );

            let log_store = MemLogStore::new();
            let sm = HpcStateMachine::new(Arc::clone(&state));

            let raft = openraft::Raft::new(id, config, network_factory.clone(), log_store, sm)
                .await
                .unwrap();

            network_factory.register(id, raft.clone()).await;
            nodes.push((raft, state));
        }

        nodes[0].0.initialize(members).await.unwrap();

        nodes[0]
            .0
            .wait(None)
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .unwrap();

        nodes
    }

    /// Create a multi-node gRPC cluster for testing.
    async fn create_test_grpc_cluster(
        node_count: u64,
    ) -> (
        Vec<(openraft::Raft<TestTypeConfig>, Arc<RwLock<TestState>>)>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let mut listeners = Vec::new();
        let mut addresses = Vec::new();
        for _ in 0..node_count {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addresses.push(addr.to_string());
            listeners.push(listener);
        }

        let network_factory = GrpcNetworkFactory::new();
        let mut members = BTreeMap::new();
        let mut nodes = Vec::new();
        let mut server_handles = Vec::new();

        for (i, addr) in addresses.iter().enumerate() {
            let id = (i + 1) as u64;
            members.insert(id, openraft::impls::BasicNode::new(addr.clone()));
            network_factory.register(id, addr.clone()).await;
        }

        for (i, listener) in listeners.into_iter().enumerate() {
            let id = (i + 1) as u64;
            let state = Arc::new(RwLock::new(TestState::default()));
            let config = Arc::new(
                openraft::Config {
                    heartbeat_interval: 200,
                    election_timeout_min: 500,
                    election_timeout_max: 1000,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            );

            let log_store = MemLogStore::new();
            let sm = HpcStateMachine::new(Arc::clone(&state));

            let raft = openraft::Raft::new(id, config, network_factory.clone(), log_store, sm)
                .await
                .unwrap();

            let server = RaftTransportServer::new(raft.clone());
            let handle = tokio::spawn(async move {
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                let _ = tonic::transport::Server::builder()
                    .add_service(proto::raft_service_server::RaftServiceServer::new(server))
                    .serve_with_incoming(incoming)
                    .await;
            });
            server_handles.push(handle);

            nodes.push((raft, state));
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        nodes[0].0.initialize(members).await.unwrap();

        nodes[0]
            .0
            .wait(None)
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .unwrap();

        (nodes, server_handles)
    }

    #[tokio::test]
    async fn single_node_quorum_works() {
        let (raft, state) = create_test_quorum().await;

        // Write a value through Raft
        let cmd = TestCommand::Set("key1".into(), "value1".into());
        raft.client_write(cmd).await.unwrap();

        // Read it back from state
        let s = state.read().await;
        assert_eq!(s.data.get("key1").unwrap(), "value1");
    }

    #[tokio::test]
    async fn three_node_cluster_works() {
        let nodes = create_test_cluster(3).await;
        let (leader, state) = &nodes[0];

        // Write through leader
        let cmd = TestCommand::Set("k".into(), "v".into());
        leader.client_write(cmd).await.unwrap();

        // Give time for replication
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Read from leader state
        let s = state.read().await;
        assert_eq!(s.data.get("k").unwrap(), "v");

        // Verify replicated to followers
        for (_, fstate) in &nodes[1..] {
            let s = fstate.read().await;
            assert!(
                s.data.contains_key("k"),
                "Data should be replicated to all nodes"
            );
        }
    }

    #[tokio::test]
    #[ignore = "slow: spins up 3-node gRPC Raft cluster"]
    async fn grpc_three_node_cluster_leader_election() {
        let (nodes, handles) = create_test_grpc_cluster(3).await;
        let (leader, state) = &nodes[0];

        // Write to prove the cluster is functional
        let cmd = TestCommand::Set("grpc-key".into(), "grpc-val".into());
        leader.client_write(cmd).await.unwrap();

        let s = state.read().await;
        assert_eq!(s.data.get("grpc-key").unwrap(), "grpc-val");

        for h in handles {
            h.abort();
        }
    }

    #[tokio::test]
    #[ignore = "slow: spins up 3-node gRPC Raft cluster"]
    async fn grpc_three_node_cluster_log_replication() {
        let (nodes, handles) = create_test_grpc_cluster(3).await;
        let (leader, _) = &nodes[0];

        // Write through leader
        let cmd = TestCommand::Set("replicated".into(), "yes".into());
        leader.client_write(cmd).await.unwrap();

        // Give time for replication
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Verify state replicated to followers
        for (_, state) in &nodes[1..] {
            let s = state.read().await;
            assert!(
                s.data.contains_key("replicated"),
                "Data should be replicated to all nodes"
            );
        }

        for h in handles {
            h.abort();
        }
    }
}
