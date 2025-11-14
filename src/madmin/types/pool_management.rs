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
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Decommissioning information for a pool
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolDecommissionInfo {
    /// Time when decommissioning started
    #[serde(rename = "startTime")]
    pub start_time: DateTime<Utc>,
    /// Initial pool size in bytes
    #[serde(rename = "startSize")]
    pub start_size: i64,
    /// Total pool size in bytes
    #[serde(rename = "totalSize")]
    pub total_size: i64,
    /// Current pool size in bytes
    #[serde(rename = "currentSize")]
    pub current_size: i64,
    /// Whether decommissioning is complete
    #[serde(rename = "complete")]
    pub complete: bool,
    /// Whether decommissioning failed
    #[serde(rename = "failed")]
    pub failed: bool,
    /// Whether decommissioning was canceled
    #[serde(rename = "canceled")]
    pub canceled: bool,
    /// Number of objects successfully decommissioned
    #[serde(rename = "objectsDecommissioned")]
    pub objects_decommissioned: i64,
    /// Number of objects that failed decommissioning
    #[serde(rename = "objectsDecommissionedFailed")]
    pub objects_decommission_failed: i64,
    /// Bytes successfully decommissioned
    #[serde(rename = "bytesDecommissioned")]
    pub bytes_done: i64,
    /// Bytes that failed decommissioning
    #[serde(rename = "bytesDecommissionedFailed")]
    pub bytes_failed: i64,
}

impl PoolDecommissionInfo {
    /// Calculate percentage of decommissioning completion
    pub fn percent_complete(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        let done = self.start_size - self.current_size;
        (done as f64 / self.total_size as f64) * 100.0
    }

    /// Check if decommissioning is in progress
    pub fn is_in_progress(&self) -> bool {
        !self.complete && !self.failed && !self.canceled
    }
}

/// Status of a storage pool
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolStatus {
    /// Pool ID
    #[serde(rename = "id")]
    pub id: i32,
    /// Pool command line definition
    #[serde(rename = "cmdline")]
    pub cmdline: String,
    /// Last status update time
    #[serde(rename = "lastUpdate")]
    pub last_update: DateTime<Utc>,
    /// Decommission information (if pool is being decommissioned)
    #[serde(rename = "decommissionInfo", skip_serializing_if = "Option::is_none")]
    pub decommission: Option<PoolDecommissionInfo>,
}

impl PoolStatus {
    /// Check if pool is being decommissioned
    pub fn is_decommissioning(&self) -> bool {
        self.decommission
            .as_ref()
            .is_some_and(|d| d.is_in_progress())
    }
}

#[async_trait]
impl FromMadminResponse for PoolStatus {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let text = response.text().await.map_err(ValidationErr::HttpError)?;
        serde_json::from_str(&text).map_err(|e| ValidationErr::JsonError(e).into())
    }
}

#[async_trait]
impl FromMadminResponse for Vec<PoolStatus> {
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
    fn test_pool_status_deserialization() {
        let json = r#"{
            "id": 0,
            "cmdline": "http://server{1...4}/disk{1...4}",
            "lastUpdate": "2025-11-07T12:00:00Z"
        }"#;
        let status: PoolStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.id, 0);
        assert_eq!(status.cmdline, "http://server{1...4}/disk{1...4}");
        assert!(status.decommission.is_none());
        assert!(!status.is_decommissioning());
    }

    #[test]
    fn test_pool_status_with_decommission() {
        let json = r#"{
            "id": 1,
            "cmdline": "http://server{5...8}/disk{1...4}",
            "lastUpdate": "2025-11-07T12:00:00Z",
            "decommissionInfo": {
                "startTime": "2025-11-07T10:00:00Z",
                "startSize": 1000000000,
                "totalSize": 1000000000,
                "currentSize": 500000000,
                "complete": false,
                "failed": false,
                "canceled": false,
                "objectsDecommissioned": 1000,
                "objectsDecommissionedFailed": 10,
                "bytesDecommissioned": 500000000,
                "bytesDecommissionedFailed": 1000000
            }
        }"#;
        let status: PoolStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.id, 1);
        assert!(status.decommission.is_some());
        assert!(status.is_decommissioning());

        let decom = status.decommission.unwrap();
        assert_eq!(decom.start_size, 1000000000);
        assert_eq!(decom.current_size, 500000000);
        assert!(decom.is_in_progress());
        assert_eq!(decom.percent_complete(), 50.0);
    }

    #[test]
    fn test_pool_decommission_percent_complete() {
        let decom = PoolDecommissionInfo {
            start_time: Utc::now(),
            start_size: 1000,
            total_size: 1000,
            current_size: 250,
            complete: false,
            failed: false,
            canceled: false,
            objects_decommissioned: 750,
            objects_decommission_failed: 0,
            bytes_done: 750000,
            bytes_failed: 0,
        };
        assert_eq!(decom.percent_complete(), 75.0);
    }

    #[test]
    fn test_list_pools_status_deserialization() {
        let json = r#"[
            {"id": 0, "cmdline": "pool1", "lastUpdate": "2025-11-07T12:00:00Z"},
            {"id": 1, "cmdline": "pool2", "lastUpdate": "2025-11-07T12:00:00Z"}
        ]"#;
        let pools: Vec<PoolStatus> = serde_json::from_str(json).unwrap();
        assert_eq!(pools.len(), 2);
        assert_eq!(pools[0].id, 0);
        assert_eq!(pools[1].id, 1);
    }
}
