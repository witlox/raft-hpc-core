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
use serde::de::DeserializeOwned;
use serde::Serialize;

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
pub use backup::{export_backup, restore_backup, verify_backup, BackupMetadata};
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
                TestCommand::Set(k, v) => write!(f, "Set({k}, {v})"),
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
}
