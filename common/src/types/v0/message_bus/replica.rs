use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use strum_macros::{EnumString, ToString};

/// Get all the replicas from specific node and pool
/// or None for all nodes or all pools
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetReplicas {
    /// Filter request
    pub filter: Filter,
}

/// Replica information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Replica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// id of the pool
    pub pool: PoolId,
    /// thin provisioning
    pub thin: bool,
    /// size of the replica in bytes
    pub size: u64,
    /// protocol used for exposing the replica
    pub share: Protocol,
    /// uri usable by nexus to access it
    pub uri: String,
    /// state of the replica
    pub state: ReplicaState,
}

impl UuidString for Replica {
    fn uuid_as_string(&self) -> String {
        self.uuid.clone().into()
    }
}

bus_impl_string_uuid!(ReplicaId, "UUID of a mayastor pool replica");

impl From<Replica> for DestroyReplica {
    fn from(replica: Replica) -> Self {
        Self {
            node: replica.node,
            pool: replica.pool,
            uuid: replica.uuid,
        }
    }
}

/// Create Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// id of the pool
    pub pool: PoolId,
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
    /// Managed by our control plane
    pub managed: bool,
    /// Owners of the resource
    pub owners: ReplicaOwners,
}

/// Replica owners which is a volume or none and a list of nexuses
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct ReplicaOwners {
    volume: Option<VolumeId>,
    nexuses: Vec<NexusId>,
}
impl ReplicaOwners {
    /// Check if this replica is owned by any nexuses or a volume
    pub fn is_owned(&self) -> bool {
        self.volume.is_some() || !self.nexuses.is_empty()
    }
    /// Check if this replica is owned by this volume
    pub fn owned_by(&self, id: &VolumeId) -> bool {
        self.volume.as_ref() == Some(id)
    }
    /// Create new owners from the volume Id
    pub fn new(volume: &VolumeId) -> Self {
        Self {
            volume: Some(volume.clone()),
            nexuses: vec![],
        }
    }
    /// The replica is no longer part of the volume
    pub fn disowned_by_volume(&mut self) {
        let _ = self.volume.take();
    }
}

/// Destroy Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub pool: PoolId,
    /// uuid of the replica
    pub uuid: ReplicaId,
}

/// Share Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ShareReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub pool: PoolId,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// protocol used for exposing the replica
    pub protocol: ReplicaShareProtocol,
}

impl From<ShareReplica> for UnshareReplica {
    fn from(share: ShareReplica) -> Self {
        Self {
            node: share.node,
            pool: share.pool,
            uuid: share.uuid,
        }
    }
}
impl From<&Replica> for ShareReplica {
    fn from(from: &Replica) -> Self {
        Self {
            node: from.node.clone(),
            pool: from.pool.clone(),
            uuid: from.uuid.clone(),
            protocol: ReplicaShareProtocol::Nvmf,
        }
    }
}
impl From<&Replica> for UnshareReplica {
    fn from(from: &Replica) -> Self {
        Self {
            node: from.node.clone(),
            pool: from.pool.clone(),
            uuid: from.uuid.clone(),
        }
    }
}
impl From<UnshareReplica> for ShareReplica {
    fn from(share: UnshareReplica) -> Self {
        Self {
            node: share.node,
            pool: share.pool,
            uuid: share.uuid,
            protocol: ReplicaShareProtocol::Nvmf,
        }
    }
}

/// Unshare Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnshareReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub pool: PoolId,
    /// uuid of the replica
    pub uuid: ReplicaId,
}

/// The protocol used to share the replica.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, EnumString, ToString, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaShareProtocol {
    /// shared as NVMe-oF TCP
    Nvmf = 1,
}

impl std::cmp::PartialEq<Protocol> for ReplicaShareProtocol {
    fn eq(&self, other: &Protocol) -> bool {
        &Protocol::from(*self) == other
    }
}
impl Default for ReplicaShareProtocol {
    fn default() -> Self {
        Self::Nvmf
    }
}
impl From<i32> for ReplicaShareProtocol {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Nvmf,
            _ => panic!("Invalid replica share protocol {}", src),
        }
    }
}
impl From<ReplicaShareProtocol> for Protocol {
    fn from(src: ReplicaShareProtocol) -> Self {
        match src {
            ReplicaShareProtocol::Nvmf => Self::Nvmf,
        }
    }
}

/// State of the Replica
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaState {
    /// unknown state
    Unknown = 0,
    /// the replica is in normal working order
    Online = 1,
    /// the replica has experienced a failure but can still function
    Degraded = 2,
    /// the replica is completely inaccessible
    Faulted = 3,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for ReplicaState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}
