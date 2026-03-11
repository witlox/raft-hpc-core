//! gRPC-based Raft network transport for multi-process clusters.
//!
//! Implements openraft's `RaftNetworkFactory` and `RaftNetworkV2` traits
//! using tonic gRPC for inter-node communication. Each Raft message is
//! JSON-serialized into a bytes payload and transported via the `RaftService` proto.
//!
//! ## TLS
//!
//! When a [`PeerTlsConfig`] is provided to [`GrpcNetworkFactory::with_tls`],
//! all peer connections use TLS with optional mTLS (client certificate).

use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::error::{RPCError, StreamingError, Unreachable};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{Backoff, RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::RaftTypeConfig;

use tokio::sync::RwLock;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tracing::{debug, warn};

use crate::proto::raft_service_client::RaftServiceClient;
use crate::proto::{RaftPayload, SnapshotRequest};

// ─── Peer TLS configuration ─────────────────────────────────────────────────

/// TLS configuration for Raft peer-to-peer connections (client side).
#[derive(Debug, Clone)]
pub struct PeerTlsConfig {
    /// CA certificate PEM for verifying peer server certificates.
    pub ca_cert_pem: Vec<u8>,
    /// Optional client identity (cert + key) for mTLS.
    pub client_identity: Option<(Vec<u8>, Vec<u8>)>,
    /// Domain name override for TLS verification (useful when peers use IPs).
    pub domain_override: Option<String>,
}

impl PeerTlsConfig {
    /// Load a peer TLS config from file paths.
    pub fn from_paths(
        ca_path: &PathBuf,
        client_cert_path: Option<&PathBuf>,
        client_key_path: Option<&PathBuf>,
        domain_override: Option<String>,
    ) -> Result<Self, io::Error> {
        let ca_cert_pem = std::fs::read(ca_path)?;
        let client_identity = match (client_cert_path, client_key_path) {
            (Some(cert), Some(key)) => {
                let cert_pem = std::fs::read(cert)?;
                let key_pem = std::fs::read(key)?;
                Some((cert_pem, key_pem))
            }
            _ => None,
        };
        Ok(Self {
            ca_cert_pem,
            client_identity,
            domain_override,
        })
    }

    /// Build a `tonic::transport::ClientTlsConfig` from this configuration.
    fn to_tonic_client_tls(&self) -> ClientTlsConfig {
        let mut tls =
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&self.ca_cert_pem));

        if let Some(ref domain) = self.domain_override {
            tls = tls.domain_name(domain);
        }

        if let Some((ref cert, ref key)) = self.client_identity {
            tls = tls.identity(Identity::from_pem(cert, key));
        }

        tls
    }
}

// ─── Network factory ────────────────────────────────────────────────────────

/// Maps node IDs to their gRPC addresses for connection management.
#[derive(Clone)]
pub struct GrpcNetworkFactory {
    addresses: Arc<RwLock<HashMap<u64, String>>>,
    /// Optional TLS configuration for peer connections.
    tls: Option<PeerTlsConfig>,
}

impl GrpcNetworkFactory {
    pub fn new() -> Self {
        Self {
            addresses: Arc::new(RwLock::new(HashMap::new())),
            tls: None,
        }
    }

    /// Create a new factory with TLS enabled for peer connections.
    pub fn with_tls(tls: PeerTlsConfig) -> Self {
        Self {
            addresses: Arc::new(RwLock::new(HashMap::new())),
            tls: Some(tls),
        }
    }

    /// Register a node's address for gRPC connections.
    pub async fn register(&self, id: u64, address: String) {
        let mut addrs = self.addresses.write().await;
        addrs.insert(id, address);
    }
}

impl Default for GrpcNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> RaftNetworkFactory<C> for GrpcNetworkFactory
where
    C: RaftTypeConfig<
        NodeId = u64,
        Node = openraft::impls::BasicNode,
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
{
    type Network = GrpcNetwork;

    async fn new_client(
        &mut self,
        target: u64,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        // Prefer the address from the BasicNode (which comes from cluster membership),
        // fall back to the registered address.
        let address = {
            let addr = node.addr.clone();
            if addr.is_empty() {
                let addrs = self.addresses.read().await;
                addrs.get(&target).cloned().unwrap_or_default()
            } else {
                addr
            }
        };

        GrpcNetwork {
            target,
            address,
            client: None,
            tls: self.tls.clone(),
        }
    }
}

/// A gRPC connection to a specific Raft peer node.
pub struct GrpcNetwork {
    target: u64,
    address: String,
    client: Option<RaftServiceClient<Channel>>,
    /// Optional TLS configuration for this connection.
    tls: Option<PeerTlsConfig>,
}

impl GrpcNetwork {
    async fn get_client<C: RaftTypeConfig>(
        &mut self,
    ) -> Result<&mut RaftServiceClient<Channel>, RPCError<C>> {
        if self.client.is_none() {
            let use_tls = self.tls.is_some();
            let endpoint = if self.address.starts_with("http") {
                self.address.clone()
            } else if use_tls {
                format!("https://{}", self.address)
            } else {
                format!("http://{}", self.address)
            };

            debug!(
                "Connecting to Raft peer {} at {} (TLS={})",
                self.target, endpoint, use_tls
            );

            let mut channel_builder = Channel::from_shared(endpoint).map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Invalid address for node {}: {}", self.target, e),
                )))
            })?;

            if let Some(ref tls_cfg) = self.tls {
                let client_tls = tls_cfg.to_tonic_client_tls();
                channel_builder = channel_builder.tls_config(client_tls).map_err(|e| {
                    RPCError::Unreachable(Unreachable::new(&io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("TLS config error for node {}: {}", self.target, e),
                    )))
                })?;
            }

            let channel = channel_builder.connect().await.map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("Cannot connect to node {}: {}", self.target, e),
                )))
            })?;

            self.client = Some(RaftServiceClient::new(channel));
        }

        Ok(self.client.as_mut().unwrap())
    }

    /// Reset the connection so the next call reconnects.
    fn reset_client(&mut self) {
        self.client = None;
    }
}

impl<C> RaftNetworkV2<C> for GrpcNetwork
where
    C: RaftTypeConfig<
        NodeId = u64,
        Node = openraft::impls::BasicNode,
        Entry = openraft::Entry<C>,
        Vote = openraft::vote::Vote<C>,
        SnapshotData = Cursor<Vec<u8>>,
    >,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let data = serde_json::to_vec(&rpc).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize AppendEntries: {e}"),
            )))
        })?;

        let client = self.get_client::<C>().await?;
        let resp = client
            .append_entries(RaftPayload { data })
            .await
            .map_err(|e| {
                warn!("AppendEntries RPC to node {} failed: {}", self.target, e);
                self.reset_client();
                RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                    "AppendEntries RPC failed: {e}"
                ))))
            })?;

        serde_json::from_slice(&resp.into_inner().data).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize AppendEntries response: {e}"),
            )))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let data = serde_json::to_vec(&rpc).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize Vote: {e}"),
            )))
        })?;

        let client = self.get_client::<C>().await?;
        let resp = client.vote(RaftPayload { data }).await.map_err(|e| {
            warn!("Vote RPC to node {} failed: {}", self.target, e);
            self.reset_client();
            RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                "Vote RPC failed: {e}"
            ))))
        })?;

        serde_json::from_slice(&resp.into_inner().data).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize Vote response: {e}"),
            )))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: C::Vote,
        snapshot: Snapshot<C>,
        _cancel: impl futures::Future<Output = openraft::errors::ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let vote_bytes = serde_json::to_vec(&vote).map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize vote: {e}"),
            )))
        })?;

        let meta_bytes = serde_json::to_vec(&snapshot.meta).map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize snapshot meta: {e}"),
            )))
        })?;

        let snapshot_data = snapshot.snapshot.into_inner();

        let client = self.get_client::<C>().await.map_err(|e| match e {
            RPCError::Unreachable(u) => StreamingError::Unreachable(u),
            _ => StreamingError::Unreachable(Unreachable::new(&io::Error::other(
                "Connection failed",
            ))),
        })?;

        let resp = client
            .install_snapshot(SnapshotRequest {
                vote: vote_bytes,
                meta: meta_bytes,
                snapshot_data,
            })
            .await
            .map_err(|e| {
                warn!("InstallSnapshot RPC to node {} failed: {}", self.target, e);
                self.reset_client();
                StreamingError::Unreachable(Unreachable::new(&io::Error::other(format!(
                    "InstallSnapshot RPC failed: {e}"
                ))))
            })?;

        serde_json::from_slice(&resp.into_inner().data).map_err(|e| {
            StreamingError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize snapshot response: {e}"),
            )))
        })
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(std::time::Duration::from_millis(200)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_types::TestTypeConfig;
    use openraft::raft::{AppendEntriesRequest, VoteRequest};

    #[test]
    fn append_entries_request_roundtrip() {
        let req: AppendEntriesRequest<TestTypeConfig> = AppendEntriesRequest {
            vote: openraft::vote::Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let data = serde_json::to_vec(&req).unwrap();
        let decoded: AppendEntriesRequest<TestTypeConfig> = serde_json::from_slice(&data).unwrap();
        assert_eq!(decoded.vote, req.vote);
    }

    #[test]
    fn vote_request_roundtrip() {
        let req: VoteRequest<TestTypeConfig> = VoteRequest {
            vote: openraft::vote::Vote::new(2, 3),
            last_log_id: None,
        };
        let data = serde_json::to_vec(&req).unwrap();
        let decoded: VoteRequest<TestTypeConfig> = serde_json::from_slice(&data).unwrap();
        assert_eq!(decoded.vote, req.vote);
    }

    #[test]
    fn grpc_network_factory_default() {
        let factory = GrpcNetworkFactory::default();
        assert!(Arc::strong_count(&factory.addresses) == 1);
        assert!(factory.tls.is_none());
    }

    #[tokio::test]
    async fn register_peer_address() {
        let factory = GrpcNetworkFactory::new();
        factory.register(1, "127.0.0.1:9000".to_string()).await;
        factory.register(2, "127.0.0.1:9001".to_string()).await;

        let addrs = factory.addresses.read().await;
        assert_eq!(addrs.get(&1).unwrap(), "127.0.0.1:9000");
        assert_eq!(addrs.get(&2).unwrap(), "127.0.0.1:9001");
    }

    #[test]
    fn grpc_network_factory_with_tls() {
        let tls_cfg = PeerTlsConfig {
            ca_cert_pem: b"dummy-ca-cert".to_vec(),
            client_identity: None,
            domain_override: Some("raft.hpc.local".to_string()),
        };
        let factory = GrpcNetworkFactory::with_tls(tls_cfg);
        assert!(factory.tls.is_some());
        assert_eq!(
            factory.tls.as_ref().unwrap().domain_override.as_deref(),
            Some("raft.hpc.local")
        );
    }

    #[test]
    fn peer_tls_config_builds_client_tls() {
        let tls_cfg = PeerTlsConfig {
            ca_cert_pem: b"-----BEGIN CERTIFICATE-----\nMIIBfTCCASOgAwIBAgIRAK...\n-----END CERTIFICATE-----\n".to_vec(),
            client_identity: Some((
                b"-----BEGIN CERTIFICATE-----\nclient-cert\n-----END CERTIFICATE-----\n".to_vec(),
                b"-----BEGIN PRIVATE KEY-----\nclient-key\n-----END PRIVATE KEY-----\n".to_vec(),
            )),
            domain_override: Some("peer.hpc.local".to_string()),
        };
        // to_tonic_client_tls should not panic (it just constructs the config)
        let _client_tls = tls_cfg.to_tonic_client_tls();
    }

    #[tokio::test]
    async fn factory_with_tls_creates_tls_enabled_network() {
        let tls_cfg = PeerTlsConfig {
            ca_cert_pem: b"dummy-ca".to_vec(),
            client_identity: None,
            domain_override: None,
        };
        let mut factory = GrpcNetworkFactory::with_tls(tls_cfg);

        let node = openraft::impls::BasicNode {
            addr: "10.0.0.1:9000".to_string(),
        };
        let network = RaftNetworkFactory::<TestTypeConfig>::new_client(&mut factory, 1, &node).await;
        assert!(network.tls.is_some(), "network should inherit TLS config");
        assert_eq!(network.address, "10.0.0.1:9000");
    }
}
