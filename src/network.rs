//! In-memory Raft network for testing.
//!
//! Provides a channel-based network where each node registers its `Raft`
//! handle, and messages are dispatched directly without serialization.

use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::sync::Arc;

use openraft::error::{RPCError, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{Backoff, RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{Raft, RaftTypeConfig};
use tokio::sync::RwLock;

/// A network factory that routes messages through in-memory channels.
/// Each node registers its `Raft` handle, and messages are dispatched directly.
#[derive(Clone)]
pub struct MemNetworkFactory<C: RaftTypeConfig> {
    pub routers: Arc<RwLock<HashMap<C::NodeId, Raft<C>>>>,
}

impl<C: RaftTypeConfig> MemNetworkFactory<C> {
    pub fn new() -> Self {
        Self {
            routers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, id: C::NodeId, raft: Raft<C>) {
        let mut routers = self.routers.write().await;
        routers.insert(id, raft);
    }
}

impl<C: RaftTypeConfig> Default for MemNetworkFactory<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> RaftNetworkFactory<C> for MemNetworkFactory<C>
where
    C: RaftTypeConfig<
        Vote = openraft::vote::Vote<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
{
    type Network = MemNetwork<C>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        MemNetwork {
            target,
            routers: Arc::clone(&self.routers),
        }
    }
}

/// A network connection to a specific peer node, routing through the in-memory router table.
pub struct MemNetwork<C: RaftTypeConfig> {
    target: C::NodeId,
    routers: Arc<RwLock<HashMap<C::NodeId, Raft<C>>>>,
}

impl<C> RaftNetworkV2<C> for MemNetwork<C>
where
    C: RaftTypeConfig<
        Vote = openraft::vote::Vote<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let routers = self.routers.read().await;
        let raft = routers.get(&self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                format!("Node {:?} not found in router table", self.target),
            )))
        })?;

        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let routers = self.routers.read().await;
        let raft = routers.get(&self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                format!("Node {:?} not found in router table", self.target),
            )))
        })?;

        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: C::Vote,
        snapshot: Snapshot<C>,
        _cancel: impl futures::Future<Output = openraft::errors::ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let routers = self.routers.read().await;
        let raft = routers.get(&self.target).ok_or_else(|| {
            StreamingError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotConnected,
                format!("Node {:?} not found in router table", self.target),
            )))
        })?;

        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(std::time::Duration::from_millis(100)))
    }
}
