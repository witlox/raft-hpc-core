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
    C: RaftTypeConfig<
        Vote = openraft::vote::Vote<C>,
        SnapshotData = Cursor<Vec<u8>>,
    > + 'static,
{
    async fn append_entries(
        &self,
        request: Request<RaftPayload>,
    ) -> Result<Response<RaftPayload>, Status> {
        let req: AppendEntriesRequest<C> =
            serde_json::from_slice(&request.into_inner().data)
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
        use openraft::vote::leader_id_adv::CommittedLeaderId;
        use openraft::vote::RaftLeaderId;

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
        let decoded: openraft::vote::Vote<TestTypeConfig> =
            serde_json::from_slice(&data).unwrap();
        assert_eq!(decoded, vote);
    }
}
