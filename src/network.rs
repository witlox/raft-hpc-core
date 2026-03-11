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
    C: RaftTypeConfig<Vote = openraft::vote::Vote<C>, SnapshotData = Cursor<Vec<u8>>>,
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
    C: RaftTypeConfig<Vote = openraft::vote::Vote<C>, SnapshotData = Cursor<Vec<u8>>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_types::TestTypeConfig;

    #[test]
    fn default_factory() {
        let factory = MemNetworkFactory::<TestTypeConfig>::default();
        assert!(Arc::strong_count(&factory.routers) == 1);
    }

    #[tokio::test]
    async fn register_and_create_client() {
        use openraft::network::RaftNetworkFactory;

        let mut factory = MemNetworkFactory::<TestTypeConfig>::new();

        // Create a minimal Raft instance to register
        let state = Arc::new(RwLock::new(crate::test_types::TestState::default()));
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
        let log_store = crate::store::MemLogStore::new();
        let sm = crate::state_machine::HpcStateMachine::new(state);
        let network = MemNetworkFactory::new();

        let raft = openraft::Raft::new(1, config, network, log_store, sm)
            .await
            .unwrap();

        factory.register(1, raft).await;

        let routers = factory.routers.read().await;
        assert!(routers.contains_key(&1));
        drop(routers);

        let node = openraft::impls::BasicNode::new("127.0.0.1:0");
        let _network = factory.new_client(1, &node).await;
    }

    #[tokio::test]
    async fn mem_network_backoff() {
        use openraft::network::v2::RaftNetworkV2;

        let factory = MemNetworkFactory::<TestTypeConfig>::new();
        let network = MemNetwork {
            target: 1u64,
            routers: Arc::clone(&factory.routers),
        };
        let _backoff = network.backoff();
    }

    #[tokio::test]
    async fn mem_network_vote_missing_node() {
        use openraft::network::RPCOption;
        use openraft::network::v2::RaftNetworkV2;

        let factory = MemNetworkFactory::<TestTypeConfig>::new();
        let mut network = MemNetwork {
            target: 99u64,
            routers: Arc::clone(&factory.routers),
        };

        let req = openraft::raft::VoteRequest {
            vote: openraft::vote::Vote::new(1, 1),
            last_log_id: None,
        };
        let result = network
            .vote(req, RPCOption::new(std::time::Duration::from_secs(1)))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn mem_network_append_entries_missing_node() {
        use openraft::network::RPCOption;
        use openraft::network::v2::RaftNetworkV2;

        let factory = MemNetworkFactory::<TestTypeConfig>::new();
        let mut network = MemNetwork {
            target: 99u64,
            routers: Arc::clone(&factory.routers),
        };

        let req = openraft::raft::AppendEntriesRequest {
            vote: openraft::vote::Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let result = network
            .append_entries(req, RPCOption::new(std::time::Duration::from_secs(1)))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn mem_network_full_snapshot_missing_node() {
        use openraft::network::RPCOption;
        use openraft::network::v2::RaftNetworkV2;
        use openraft::storage::Snapshot;

        let factory = MemNetworkFactory::<TestTypeConfig>::new();
        let mut network = MemNetwork {
            target: 99u64,
            routers: Arc::clone(&factory.routers),
        };

        let vote = openraft::vote::Vote::new(1, 1);
        let snapshot = Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: None,
                last_membership: openraft::StoredMembership::default(),
                snapshot_id: "test".to_string(),
            },
            snapshot: std::io::Cursor::new(vec![]),
        };
        let cancel = futures::future::pending();
        let result = network
            .full_snapshot(
                vote,
                snapshot,
                cancel,
                RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await;
        assert!(result.is_err());
    }
}
