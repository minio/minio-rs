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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Progress information for rebalance operation on a pool
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalPoolProgress {
    /// Number of objects processed
    #[serde(rename = "NumObjects")]
    pub num_objects: u64,
    /// Number of versions processed
    #[serde(rename = "NumVersions")]
    pub num_versions: u64,
    /// Number of bytes processed
    #[serde(rename = "Bytes")]
    pub bytes: u64,
    /// Current bucket being processed
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Current object being processed
    #[serde(rename = "Object")]
    pub object: String,
    /// Elapsed time in nanoseconds
    #[serde(rename = "Elapsed")]
    pub elapsed_nanos: i64,
    /// Estimated time remaining in nanoseconds
    #[serde(rename = "ETA")]
    pub eta_nanos: i64,
}

impl RebalPoolProgress {
    /// Get elapsed time as Duration
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.elapsed_nanos.max(0) as u64)
    }

    /// Get ETA as Duration
    pub fn eta(&self) -> Duration {
        Duration::from_nanos(self.eta_nanos.max(0) as u64)
    }
}

/// Metrics of a rebalance operation on a given pool
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RebalancePoolStatus {
    /// Pool index (zero-based)
    #[serde(rename = "id")]
    pub id: i32,
    /// Status - "Active" if rebalance is running, empty otherwise
    #[serde(rename = "status")]
    pub status: String,
    /// Percentage of used space
    #[serde(rename = "used")]
    pub used: f64,
    /// Progress information (empty when rebalance is not running)
    #[serde(rename = "progress", skip_serializing_if = "Option::is_none")]
    pub progress: Option<RebalPoolProgress>,
}

/// Status of cluster rebalance operation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RebalanceStatus {
    /// Unique identifier for the rebalance operation
    #[serde(rename = "ID")]
    pub id: String,
    /// Time when rebalance was stopped (if stopped)
    #[serde(rename = "stoppedAt", skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<DateTime<Utc>>,
    /// Status of each pool in the cluster
    #[serde(rename = "pools")]
    pub pools: Vec<RebalancePoolStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebal_pool_progress_duration_conversions() {
        let progress = RebalPoolProgress {
            num_objects: 100,
            num_versions: 200,
            bytes: 1024000,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            elapsed_nanos: 5_000_000_000, // 5 seconds
            eta_nanos: 10_000_000_000,    // 10 seconds
        };

        assert_eq!(progress.elapsed(), Duration::from_secs(5));
        assert_eq!(progress.eta(), Duration::from_secs(10));
    }

    #[test]
    fn test_rebalance_pool_status_serialization() {
        let status = RebalancePoolStatus {
            id: 0,
            status: "Active".to_string(),
            used: 75.5,
            progress: Some(RebalPoolProgress {
                num_objects: 50,
                num_versions: 100,
                bytes: 512000,
                bucket: "my-bucket".to_string(),
                object: "my-object".to_string(),
                elapsed_nanos: 1_000_000_000,
                eta_nanos: 2_000_000_000,
            }),
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"id\":0"));
        assert!(json.contains("\"status\":\"Active\""));
        assert!(json.contains("\"used\":75.5"));
        assert!(json.contains("\"progress\""));
    }

    #[test]
    fn test_rebalance_status_deserialization() {
        let json = r#"{
            "ID": "abc-123-def",
            "pools": [
                {
                    "id": 0,
                    "status": "Active",
                    "used": 80.0,
                    "progress": {
                        "NumObjects": 1000,
                        "NumVersions": 2000,
                        "Bytes": 104857600,
                        "Bucket": "data-bucket",
                        "Object": "file.txt",
                        "Elapsed": 30000000000,
                        "ETA": 60000000000
                    }
                }
            ]
        }"#;

        let status: RebalanceStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.id, "abc-123-def");
        assert_eq!(status.pools.len(), 1);
        assert_eq!(status.pools[0].id, 0);
        assert_eq!(status.pools[0].status, "Active");
        assert!(status.pools[0].progress.is_some());
    }

    #[test]
    fn test_rebalance_status_with_stopped_at() {
        let json = r#"{
            "ID": "xyz-789",
            "stoppedAt": "2025-01-01T12:00:00Z",
            "pools": []
        }"#;

        let status: RebalanceStatus = serde_json::from_str(json).unwrap();
        assert_eq!(status.id, "xyz-789");
        assert!(status.stopped_at.is_some());
        assert_eq!(status.pools.len(), 0);
    }
}
