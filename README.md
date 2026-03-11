# raft-hpc-core

[![CI](https://github.com/witlox/raft-hpc-core/actions/workflows/ci.yml/badge.svg)](https://github.com/witlox/raft-hpc-core/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/raft-hpc-core.svg)](https://crates.io/crates/raft-hpc-core)
[![codecov](https://codecov.io/gh/witlox/raft-hpc-core/graph/badge.svg)](https://codecov.io/gh/witlox/raft-hpc-core)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

Shared Raft consensus infrastructure for HPC systems. This crate provides reusable components for building distributed applications using the [openraft](https://github.com/datafuselabs/openraft) library.

This crate enables multiple applications (like [Pact](https://github.com/witlox/pact) and [Lattice](https://github.com/witlox/lattice)) to share common Raft infrastructure while defining their own application-specific state and commands.

## Features

- **Log Stores**: In-memory and file-backed implementations with a polymorphic variant
- **gRPC Transport**: TLS-enabled peer-to-peer communication using Tonic
- **In-Memory Network**: Channel-based network for testing without serialization overhead
- **State Machine**: Generic state machine with persistent snapshot support
- **Backup/Restore**: Export, verify, and restore Raft state as compressed tar.gz archives

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
raft-hpc-core = "2026.1"
```

Or to use the latest development version from git:

```toml
[dependencies]
raft-hpc-core = { git = "https://github.com/witlox/raft-hpc-core" }
```

## Usage

### 1. Define Your Type Configuration

Each application provides its own `TypeConfig` via `openraft::declare_raft_types!`:

```rust
use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MyCommand {
    Set(String, String),
    Delete(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MyResponse {
    Ok,
    Value(Option<String>),
}

declare_raft_types!(
    pub MyTypeConfig:
        D = MyCommand,
        R = MyResponse,
        NodeId = u64,
        Node = openraft::impls::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);
```

### 2. Implement Your Application State

Implement `StateMachineState` for your state type:

```rust
use raft_hpc_core::StateMachineState;
use std::collections::HashMap;

#[derive(Default, Serialize, Deserialize)]
pub struct MyState {
    data: HashMap<String, String>,
}

impl StateMachineState<MyTypeConfig> for MyState {
    fn apply(&mut self, cmd: MyCommand) -> MyResponse {
        match cmd {
            MyCommand::Set(k, v) => {
                self.data.insert(k, v);
                MyResponse::Ok
            }
            MyCommand::Delete(k) => {
                self.data.remove(&k);
                MyResponse::Ok
            }
        }
    }

    fn blank_response() -> MyResponse {
        MyResponse::Ok
    }
}
```

### 3. Choose a Log Store

```rust
use raft_hpc_core::{MemLogStore, FileLogStore, LogStoreVariant};

// In-memory (for testing or ephemeral nodes)
let mem_store = MemLogStore::<MyTypeConfig>::new();

// File-backed (for production)
let file_store = FileLogStore::<MyTypeConfig>::new(&data_dir)?;

// Or use the polymorphic variant
let store = LogStoreVariant::File(file_store);
```

### 4. Set Up the Network

For production with gRPC:

```rust
use raft_hpc_core::{GrpcNetworkFactory, PeerTlsConfig, RaftTransportServer};

// Plain gRPC
let network = GrpcNetworkFactory::new();
network.register(1, "10.0.0.1:9000".to_string()).await;

// With TLS
let tls = PeerTlsConfig::from_paths(
    &ca_path,
    Some(&client_cert_path),
    Some(&client_key_path),
    Some("raft.hpc.local".to_string()),
)?;
let network = GrpcNetworkFactory::with_tls(tls);

// Server side: wrap your Raft instance
let server = RaftTransportServer::new(raft);
```

For testing with in-memory channels:

```rust
use raft_hpc_core::MemNetworkFactory;

let network = MemNetworkFactory::<MyTypeConfig>::new();
network.register(node_id, raft_handle).await;
```

### 5. Create the State Machine

```rust
use raft_hpc_core::HpcStateMachine;
use std::sync::Arc;
use tokio::sync::RwLock;

let state = Arc::new(RwLock::new(MyState::default()));

// In-memory snapshots only
let sm = HpcStateMachine::new(Arc::clone(&state));

// With persistent snapshots
let sm = HpcStateMachine::with_snapshot_dir(
    Arc::clone(&state),
    data_dir.join("snapshots"),
)?;
```

### 6. Backup and Restore

For backup support, implement `BackupMetadataSource`:

```rust
use raft_hpc_core::{BackupMetadataSource, export_backup, verify_backup, restore_backup};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyBackupMetadata {
    entry_count: usize,
}

impl BackupMetadataSource for MyState {
    type Metadata = MyBackupMetadata;

    fn backup_metadata(&self) -> MyBackupMetadata {
        MyBackupMetadata {
            entry_count: self.data.len(),
        }
    }
}

// Export
let metadata = export_backup(&state, &backup_path).await?;

// Verify
let metadata = verify_backup::<MyState, MyBackupMetadata>(&backup_path)?;

// Restore (stops the cluster first, then restart with restored data)
let metadata = restore_backup::<MyState, MyBackupMetadata>(&backup_path, &data_dir)?;
```

## Architecture

### What's Provided (Generic)

| Component | Description |
|-----------|-------------|
| `MemLogStore` | In-memory log storage |
| `FileLogStore` | File-backed WAL with JSON entries |
| `LogStoreVariant` | Polymorphic wrapper for runtime selection |
| `GrpcNetworkFactory` | gRPC transport with optional TLS/mTLS |
| `MemNetworkFactory` | In-memory network for testing |
| `RaftTransportServer` | Tonic gRPC server handler |
| `HpcStateMachine` | State machine with snapshot management |
| `export_backup` / `verify_backup` / `restore_backup` | Backup utilities |

### What You Provide (Application-Specific)

| Component | Description |
|-----------|-------------|
| `TypeConfig` | Your openraft type configuration |
| Command enum | Application commands (e.g., `Set`, `Delete`) |
| Response enum | Command responses |
| State struct | Application state implementing `StateMachineState` |
| `BackupMetadataSource` | Optional metadata for backups |

## File Layout

When using persistent storage:

```
{data_dir}/raft/
├── vote.json           # Persisted vote
├── committed.json      # Last committed log ID
├── wal/
│   ├── 1.json          # Log entry at index 1
│   ├── 2.json          # Log entry at index 2
│   └── purged.json     # Purge marker
└── snapshots/
    ├── current         # Pointer to latest snapshot
    └── snap-0-100.json # Snapshot at index 100
```

