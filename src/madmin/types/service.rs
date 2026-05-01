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

use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Service action type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceAction {
    /// Restart the MinIO service
    Restart,
    /// Stop the MinIO service
    Stop,
    /// Freeze all S3 operations
    Freeze,
    /// Unfreeze S3 operations
    Unfreeze,
    /// Cancel an ongoing restart
    #[serde(rename = "cancel-restart")]
    CancelRestart,
}

impl fmt::Display for ServiceAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceAction::Restart => write!(f, "restart"),
            ServiceAction::Stop => write!(f, "stop"),
            ServiceAction::Freeze => write!(f, "freeze"),
            ServiceAction::Unfreeze => write!(f, "unfreeze"),
            ServiceAction::CancelRestart => write!(f, "cancel-restart"),
        }
    }
}

/// Disk status information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskStatus {
    /// Disk path
    pub path: String,
    /// Disk state
    pub state: String,
    /// UUID
    #[serde(rename = "uuid")]
    pub uuid: Option<String>,
}

/// Result of service action for a single peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceActionPeerResult {
    /// Host identifier
    #[serde(rename = "host")]
    pub host: String,
    /// Error message if action failed
    #[serde(rename = "err", skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    /// Drives waiting to be formatted or healed
    #[serde(rename = "waitingDrives", skip_serializing_if = "Option::is_none")]
    pub waiting_drives: Option<HashMap<String, DiskStatus>>,
}

/// Result of a service action across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceActionResult {
    /// The action that was performed
    #[serde(rename = "action")]
    pub action: ServiceAction,
    /// Whether this was a dry run
    #[serde(rename = "dryRun")]
    pub dry_run: bool,
    /// Results from each peer in the cluster
    #[serde(rename = "results", skip_serializing_if = "Option::is_none")]
    pub results: Option<Vec<ServiceActionPeerResult>>,
}

impl ServiceActionResult {
    /// Check if all peers completed the action successfully
    pub fn all_succeeded(&self) -> bool {
        self.results
            .as_ref()
            .is_none_or(|results| results.iter().all(|r| r.err.is_none()))
    }

    /// Get list of failed peers
    pub fn failed_peers(&self) -> Vec<&ServiceActionPeerResult> {
        self.results.as_ref().map_or(Vec::new(), |results| {
            results.iter().filter(|r| r.err.is_some()).collect()
        })
    }
}

#[async_trait]
impl FromMadminResponse for ServiceActionResult {
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
    fn test_service_action_display() {
        assert_eq!(ServiceAction::Restart.to_string(), "restart");
        assert_eq!(ServiceAction::Stop.to_string(), "stop");
        assert_eq!(ServiceAction::Freeze.to_string(), "freeze");
        assert_eq!(ServiceAction::Unfreeze.to_string(), "unfreeze");
        assert_eq!(ServiceAction::CancelRestart.to_string(), "cancel-restart");
    }

    #[test]
    fn test_service_action_result_deserialization() {
        let json = r#"{
            "action": "restart",
            "dryRun": false,
            "results": [
                {"host": "server1:9000"},
                {"host": "server2:9000", "err": "connection timeout"}
            ]
        }"#;
        let result: ServiceActionResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.action, ServiceAction::Restart);
        assert!(!result.dry_run);
        assert!(!result.all_succeeded());
        assert_eq!(result.failed_peers().len(), 1);
    }

    #[test]
    fn test_service_action_result_all_succeeded() {
        let json = r#"{
            "action": "restart",
            "dryRun": true,
            "results": [
                {"host": "server1:9000"},
                {"host": "server2:9000"}
            ]
        }"#;
        let result: ServiceActionResult = serde_json::from_str(json).unwrap();
        assert!(result.all_succeeded());
        assert!(result.failed_peers().is_empty());
    }
}
