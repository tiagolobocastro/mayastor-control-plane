#![allow(
    clippy::too_many_arguments,
    clippy::new_without_default,
    non_camel_case_types,
    unused_imports
)]
/*
 * Mayastor RESTful API
 *
 * The version of the OpenAPI document: v0
 *
 * Generated by: https://github.com/openebs/openapi-generator
 */

use crate::apis::IntoVec;

/// ReplicaTopology : Location of replicas (nodes and pools)

/// Location of replicas (nodes and pools)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaTopology {
    /// storage node identifier
    #[serde(rename = "node", skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// storage pool identifier
    #[serde(rename = "pool", skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(rename = "state")]
    pub state: crate::models::ReplicaState,
}

impl ReplicaTopology {
    /// ReplicaTopology using only the required fields
    pub fn new(state: impl Into<crate::models::ReplicaState>) -> ReplicaTopology {
        ReplicaTopology {
            node: None,
            pool: None,
            state: state.into(),
        }
    }
    /// ReplicaTopology using all fields
    pub fn new_all(
        node: impl Into<Option<String>>,
        pool: impl Into<Option<String>>,
        state: impl Into<crate::models::ReplicaState>,
    ) -> ReplicaTopology {
        ReplicaTopology {
            node: node.into(),
            pool: pool.into(),
            state: state.into(),
        }
    }
}
