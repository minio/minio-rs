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
use std::collections::HashMap;

/// Options for bucket replication diff command
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplDiffOpts {
    /// Amazon Resource Name (ARN) of the replication target
    #[serde(rename = "ARN", skip_serializing_if = "Option::is_none")]
    pub arn: Option<String>,
    /// Enable verbose output
    #[serde(rename = "Verbose", default)]
    pub verbose: bool,
    /// Object prefix to filter by
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

/// Target replication status information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TgtDiffInfo {
    /// Replication status
    #[serde(rename = "rStatus", skip_serializing_if = "Option::is_none")]
    pub replication_status: Option<String>,
    /// Delete replication status
    #[serde(rename = "drStatus", skip_serializing_if = "Option::is_none")]
    pub delete_replication_status: Option<String>,
}

/// Replication status information for an object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiffInfo {
    /// Object name
    #[serde(rename = "Object")]
    pub object: String,
    /// Version ID of the object
    #[serde(rename = "VersionID")]
    pub version_id: String,
    /// Replication status per target ARN
    #[serde(rename = "Targets")]
    pub targets: HashMap<String, TgtDiffInfo>,
    /// Error message if any
    #[serde(rename = "Err", skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    /// Overall replication status
    #[serde(rename = "ReplicationStatus", skip_serializing_if = "Option::is_none")]
    pub replication_status: Option<String>,
    /// Delete marker replication status
    #[serde(
        rename = "DeleteReplicationStatus",
        skip_serializing_if = "Option::is_none"
    )]
    pub delete_replication_status: Option<String>,
    /// Timestamp of replication
    #[serde(rename = "ReplicationTimestamp")]
    pub replication_timestamp: DateTime<Utc>,
    /// Last modified time of object
    #[serde(rename = "LastModified")]
    pub last_modified: DateTime<Utc>,
    /// Whether this is a delete marker
    #[serde(rename = "IsDeleteMarker", default)]
    pub is_delete_marker: bool,
}

/// MRF (Metadata Replication Framework) backlog entry for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationMRF {
    /// Node name where the MRF entry exists
    #[serde(rename = "NodeName")]
    pub node_name: String,
    /// Bucket name
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Object name
    #[serde(rename = "Object")]
    pub object: String,
    /// Version ID of the object
    #[serde(rename = "VersionID")]
    pub version_id: String,
    /// Number of retry attempts
    #[serde(rename = "RetryCount")]
    pub retry_count: i32,
    /// Error message if replication failed
    #[serde(rename = "Err", skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repl_diff_opts_default() {
        let opts = ReplDiffOpts::default();
        assert!(opts.arn.is_none());
        assert!(!opts.verbose);
        assert!(opts.prefix.is_none());
    }

    #[test]
    fn test_repl_diff_opts_serialization() {
        let opts = ReplDiffOpts {
            arn: Some("arn:minio:replication::target1".to_string()),
            verbose: true,
            prefix: Some("documents/".to_string()),
        };

        let json = serde_json::to_string(&opts).unwrap();
        assert!(json.contains("\"ARN\""));
        assert!(json.contains("\"Verbose\":true"));
        assert!(json.contains("\"Prefix\""));
    }

    #[test]
    fn test_tgt_diff_info_serialization() {
        let info = TgtDiffInfo {
            replication_status: Some("PENDING".to_string()),
            delete_replication_status: Some("REPLICA".to_string()),
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"rStatus\":\"PENDING\""));
        assert!(json.contains("\"drStatus\":\"REPLICA\""));
    }

    #[test]
    fn test_diff_info_deserialization() {
        let json = r#"{
            "Object": "test.txt",
            "VersionID": "abc123",
            "Targets": {
                "arn:minio:replication::target1": {
                    "rStatus": "COMPLETED",
                    "drStatus": "REPLICA"
                }
            },
            "ReplicationStatus": "COMPLETED",
            "DeleteReplicationStatus": "REPLICA",
            "ReplicationTimestamp": "2025-01-01T00:00:00Z",
            "LastModified": "2025-01-01T00:00:00Z",
            "IsDeleteMarker": false
        }"#;

        let info: DiffInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.object, "test.txt");
        assert_eq!(info.version_id, "abc123");
        assert_eq!(info.targets.len(), 1);
        assert!(!info.is_delete_marker);
    }

    #[test]
    fn test_replication_mrf_deserialization() {
        let json = r#"{
            "NodeName": "node1",
            "Bucket": "mybucket",
            "Object": "myobject.txt",
            "VersionID": "v1",
            "RetryCount": 3,
            "Err": "connection timeout"
        }"#;

        let mrf: ReplicationMRF = serde_json::from_str(json).unwrap();
        assert_eq!(mrf.node_name, "node1");
        assert_eq!(mrf.bucket, "mybucket");
        assert_eq!(mrf.object, "myobject.txt");
        assert_eq!(mrf.retry_count, 3);
        assert_eq!(mrf.err, Some("connection timeout".to_string()));
    }
}
