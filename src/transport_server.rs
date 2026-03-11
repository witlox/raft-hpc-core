//! Tonic server handler for Raft RPCs.
//!
//! Receives gRPC requests, deserializes them, forwards to the local Raft
//! instance, and serializes responses back.

use std::io::Cursor;

use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::storage::Snapshot;
use openraft::{Raft, RaftTypeConfig, SnapshotMeta};
use tonic::{Request, Response, Status};

use crate::proto::raft_service_server::RaftService;
use crate::proto::{RaftPayload, SnapshotRequest};

/// Tonic service that forwards Raft RPCs to a local Raft instance.
pub struct RaftTransportServer<C: RaftTypeConfig> {
    raft: Raft<C>,
}

impl<C: RaftTypeConfig> RaftTransportServer<C> {
    pub const fn new(raft: Raft<C>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl<C> RaftService for RaftTransportServer<C>
where
    C: RaftTypeConfig<Vote = openraft::vote::Vote<C>, SnapshotData = Cursor<Vec<u8>>> + 'static,
{
    async fn append_entries(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let req: AppendEntriesRequest<C> = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("Deserialize error: {e}")))?;

        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("Raft append_entries error: {e}")))?;

        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("Serialize error: {e}")))?;

        Ok(Response::new(RaftPayload { data }))
    }

    async fn vote(&self, request: Request<RaftPayload>) -> Result<Response<RaftPayload>, Status> {
        let req: VoteRequest<C> = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("Deserialize error: {e}")))?;

        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("Raft vote error: {e}")))?;

        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("Serialize error: {e}")))?;

        Ok(Response::new(RaftPayload { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<RaftPayload>, Status> {
        let req = request.into_inner();

        let vote: C::Vote = serde_json::from_slice(&req.vote)
            .map_err(|e| Status::invalid_argument(format!("Deserialize vote error: {e}")))?;

        let meta: SnapshotMeta<C> = serde_json::from_slice(&req.meta)
            .map_err(|e| Status::invalid_argument(format!("Deserialize meta error: {e}")))?;

        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(req.snapshot_data),
        };

        let resp = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("Raft install_snapshot error: {e}")))?;

        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("Serialize error: {e}")))?;

        Ok(Response::new(RaftPayload { data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_types::TestTypeConfig;

    #[test]
    fn snapshot_meta_roundtrip() {
        use openraft::vote::RaftLeaderId;
        use openraft::vote::leader_id_adv::CommittedLeaderId;

        let meta: SnapshotMeta<TestTypeConfig> = SnapshotMeta {
            last_log_id: Some(openraft::LogId::new(CommittedLeaderId::new(1, 1), 5)),
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: "test-snap-1".to_string(),
        };
        let data = serde_json::to_vec(&meta).unwrap();
        let decoded: SnapshotMeta<TestTypeConfig> = serde_json::from_slice(&data).unwrap();
        assert_eq!(decoded.snapshot_id, "test-snap-1");
        assert_eq!(decoded.last_log_id, meta.last_log_id);
    }

    #[test]
    fn vote_roundtrip() {
        let vote = openraft::vote::Vote::<TestTypeConfig>::new(3, 7);
        let data = serde_json::to_vec(&vote).unwrap();
        let decoded: openraft::vote::Vote<TestTypeConfig> = serde_json::from_slice(&data).unwrap();
        assert_eq!(decoded, vote);
    }

    use openraft::vote::RaftLeaderId;

    async fn create_single_node_raft() -> openraft::Raft<TestTypeConfig> {
        use std::collections::BTreeMap;
        use std::sync::Arc;
        let state = Arc::new(tokio::sync::RwLock::new(
            crate::test_types::TestState::default(),
        ));
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
        let log_store = crate::MemLogStore::new();
        let sm = crate::HpcStateMachine::new(state);
        let network = crate::MemNetworkFactory::new();

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

        raft
    }

    #[tokio::test]
    async fn handler_append_entries() {
        let raft = create_single_node_raft().await;
        let server = RaftTransportServer::new(raft);

        let req: openraft::raft::AppendEntriesRequest<TestTypeConfig> =
            openraft::raft::AppendEntriesRequest {
                vote: openraft::vote::Vote::new(1, 1),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            };
        let data = serde_json::to_vec(&req).unwrap();
        let response = server
            .append_entries(Request::new(RaftPayload { data }))
            .await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn handler_vote() {
        let raft = create_single_node_raft().await;
        let server = RaftTransportServer::new(raft);

        let req: openraft::raft::VoteRequest<TestTypeConfig> = openraft::raft::VoteRequest {
            vote: openraft::vote::Vote::new(1, 1),
            last_log_id: None,
        };
        let data = serde_json::to_vec(&req).unwrap();
        let response = server.vote(Request::new(RaftPayload { data })).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn handler_install_snapshot() {
        let raft = create_single_node_raft().await;
        let server = RaftTransportServer::new(raft);

        let vote = openraft::vote::Vote::<TestTypeConfig>::new(1, 1);
        let vote_bytes = serde_json::to_vec(&vote).unwrap();

        let meta: SnapshotMeta<TestTypeConfig> = SnapshotMeta {
            last_log_id: Some(openraft::LogId::new(
                openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
                1,
            )),
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: "test-snap".to_string(),
        };
        let meta_bytes = serde_json::to_vec(&meta).unwrap();

        let state = crate::test_types::TestState::default();
        let snapshot_data = serde_json::to_vec(&state).unwrap();

        let response = server
            .install_snapshot(Request::new(SnapshotRequest {
                vote: vote_bytes,
                meta: meta_bytes,
                snapshot_data,
            }))
            .await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn handler_append_entries_invalid_data() {
        let raft = create_single_node_raft().await;
        let server = RaftTransportServer::new(raft);

        let response = server
            .append_entries(Request::new(RaftPayload {
                data: b"not json".to_vec(),
            }))
            .await;
        assert!(response.is_err());
        let status = response.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn handler_vote_invalid_data() {
        let raft = create_single_node_raft().await;
        let server = RaftTransportServer::new(raft);

        let response = server
            .vote(Request::new(RaftPayload {
                data: b"bad".to_vec(),
            }))
            .await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn handler_install_snapshot_invalid_vote() {
        let raft = create_single_node_raft().await;
        let server = RaftTransportServer::new(raft);

        let response = server
            .install_snapshot(Request::new(SnapshotRequest {
                vote: b"bad".to_vec(),
                meta: b"{}".to_vec(),
                snapshot_data: vec![],
            }))
            .await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
}
