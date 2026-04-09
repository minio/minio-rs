// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::madmin::types::service::DiskStatus;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Status of server update for a single peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerPeerUpdateStatus {
    /// Host identifier
    #[serde(rename = "host")]
    pub host: String,
    /// Error message if update failed
    #[serde(rename = "err", skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    /// Current version before update
    #[serde(rename = "currentVersion")]
    pub current_version: String,
    /// Updated version after update
    #[serde(rename = "updatedVersion")]
    pub updated_version: String,
    /// Drives waiting to be formatted or healed
    #[serde(rename = "waitingDrives", skip_serializing_if = "Option::is_none")]
    pub waiting_drives: Option<HashMap<String, DiskStatus>>,
}

/// Status of server update across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerUpdateStatus {
    /// Whether this was a dry run
    #[serde(rename = "dryRun")]
    pub dry_run: bool,
    /// Results from each peer
    #[serde(rename = "results", skip_serializing_if = "Option::is_none")]
    pub results: Option<Vec<ServerPeerUpdateStatus>>,
    /// Overall error message
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ServerUpdateStatus {
    /// Check if all peers updated successfully
    pub fn all_succeeded(&self) -> bool {
        self.error.is_none()
            && self
                .results
                .as_ref()
                .is_none_or(|r| r.iter().all(|p| p.err.is_none()))
    }
}

#[async_trait]
impl FromMadminResponse for ServerUpdateStatus {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let text = response.text().await.map_err(ValidationErr::HttpError)?;
        serde_json::from_str(&text).map_err(|e| ValidationErr::JsonError(e).into())
    }
}

/// Version information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Version {
    /// Version number
    #[serde(rename = "version")]
    pub version: u32,
}

/// API description for a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APIDesc {
    /// Backend format version
    #[serde(rename = "backendVersion")]
    pub backend_version: Version,
    /// Node API version
    #[serde(rename = "nodeAPIVersion")]
    pub node_api_version: u32,
    /// Error message if any
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Cluster API description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterAPIDesc {
    /// API description for each node
    #[serde(rename = "nodes", skip_serializing_if = "Option::is_none")]
    pub nodes: Option<HashMap<String, APIDesc>>,
    /// Overall error message
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[async_trait]
impl FromMadminResponse for ClusterAPIDesc {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let text = response.text().await.map_err(ValidationErr::HttpError)?;
        serde_json::from_str(&text).map_err(|e| ValidationErr::JsonError(e).into())
    }
}

/// Node bump version response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeBumpVersionResp {
    /// Whether version bump completed
    #[serde(rename = "done")]
    pub done: bool,
    /// Whether node is offline
    #[serde(rename = "offline")]
    pub offline: bool,
    /// Error message if any
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Cluster bump version response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterBumpVersionResp {
    /// Bump version result for each node
    #[serde(rename = "nodes", skip_serializing_if = "Option::is_none")]
    pub nodes: Option<HashMap<String, NodeBumpVersionResp>>,
    /// Overall error message
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ClusterBumpVersionResp {
    /// Check if all nodes completed successfully
    pub fn all_succeeded(&self) -> bool {
        self.error.is_none()
            && self
                .nodes
                .as_ref()
                .is_none_or(|nodes| nodes.values().all(|n| n.done && n.error.is_none()))
    }
}

#[async_trait]
impl FromMadminResponse for ClusterBumpVersionResp {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let text = response.text().await.map_err(ValidationErr::HttpError)?;
        serde_json::from_str(&text).map_err(|e| ValidationErr::JsonError(e).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_update_status_deserialization() {
        let json = r#"{"dryRun":true,"results":[{"host":"server1:9000","currentVersion":"2024.1.1","updatedVersion":"2024.2.1"}]}"#;
        let status: ServerUpdateStatus = serde_json::from_str(json).unwrap();
        assert!(status.dry_run);
        assert!(status.all_succeeded());
    }

    #[test]
    fn test_cluster_bump_version_resp() {
        let json = r#"{"nodes":{"server1":{"done":true,"offline":false}}}"#;
        let resp: ClusterBumpVersionResp = serde_json::from_str(json).unwrap();
        assert!(resp.all_succeeded());
    }
}
