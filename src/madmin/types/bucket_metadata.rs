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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Status of a metadata import operation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetaStatus {
    /// Whether this metadata was set
    #[serde(rename = "isSet")]
    pub is_set: bool,
    /// Error message if import failed
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

/// Status of bucket metadata import for various configuration elements
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BucketStatus {
    /// Object lock configuration status
    #[serde(rename = "olock", default)]
    pub object_lock: MetaStatus,
    /// Versioning configuration status
    #[serde(default)]
    pub versioning: MetaStatus,
    /// Bucket policy status
    #[serde(default)]
    pub policy: MetaStatus,
    /// Tagging configuration status
    #[serde(default)]
    pub tagging: MetaStatus,
    /// Server-side encryption configuration status
    #[serde(rename = "sse", default)]
    pub sse_config: MetaStatus,
    /// Lifecycle configuration status
    #[serde(default)]
    pub lifecycle: MetaStatus,
    /// Notification configuration status
    #[serde(default)]
    pub notification: MetaStatus,
    /// Quota configuration status
    #[serde(default)]
    pub quota: MetaStatus,
    /// CORS configuration status
    #[serde(default)]
    pub cors: MetaStatus,
    /// QoS configuration status
    #[serde(rename = "qos", default)]
    pub qos: MetaStatus,
    /// General error message for bucket-level failures
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

/// Bucket metadata import errors and status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BucketMetaImportErrs {
    /// Map of bucket name to import status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub buckets: Option<HashMap<String, BucketStatus>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_status_default() {
        let status = MetaStatus::default();
        assert!(!status.is_set);
        assert!(status.err.is_none());
    }

    #[test]
    fn test_meta_status_with_error() {
        let json = r#"{"isSet": false, "error": "permission denied"}"#;
        let status: MetaStatus = serde_json::from_str(json).unwrap();
        assert!(!status.is_set);
        assert_eq!(status.err, Some("permission denied".to_string()));
    }

    #[test]
    fn test_bucket_status_deserialization() {
        let json = r#"{
            "olock": {"isSet": true},
            "versioning": {"isSet": true},
            "policy": {"isSet": false, "error": "policy not found"},
            "tagging": {"isSet": false},
            "sse": {"isSet": false},
            "lifecycle": {"isSet": true},
            "notification": {"isSet": false},
            "quota": {"isSet": false},
            "cors": {"isSet": false},
            "qos": {"isSet": false}
        }"#;

        let status: BucketStatus = serde_json::from_str(json).unwrap();
        assert!(status.object_lock.is_set);
        assert!(status.versioning.is_set);
        assert!(!status.policy.is_set);
        assert_eq!(status.policy.err, Some("policy not found".to_string()));
        assert!(status.lifecycle.is_set);
    }

    #[test]
    fn test_bucket_meta_import_errs_empty() {
        let errs = BucketMetaImportErrs::default();
        assert!(errs.buckets.is_none());
    }

    #[test]
    fn test_bucket_meta_import_errs_with_buckets() {
        let json = r#"{
            "buckets": {
                "bucket1": {
                    "olock": {"isSet": true},
                    "versioning": {"isSet": true},
                    "policy": {"isSet": false},
                    "tagging": {"isSet": false},
                    "sse": {"isSet": false},
                    "lifecycle": {"isSet": false},
                    "notification": {"isSet": false},
                    "quota": {"isSet": false},
                    "cors": {"isSet": false},
                    "qos": {"isSet": false}
                },
                "bucket2": {
                    "olock": {"isSet": false},
                    "versioning": {"isSet": false},
                    "policy": {"isSet": false},
                    "tagging": {"isSet": false},
                    "sse": {"isSet": false},
                    "lifecycle": {"isSet": false},
                    "notification": {"isSet": false},
                    "quota": {"isSet": false},
                    "cors": {"isSet": false},
                    "qos": {"isSet": false},
                    "error": "bucket does not exist"
                }
            }
        }"#;

        let errs: BucketMetaImportErrs = serde_json::from_str(json).unwrap();
        assert!(errs.buckets.is_some());
        let buckets = errs.buckets.unwrap();
        assert_eq!(buckets.len(), 2);

        let bucket1 = &buckets["bucket1"];
        assert!(bucket1.object_lock.is_set);
        assert!(bucket1.versioning.is_set);

        let bucket2 = &buckets["bucket2"];
        assert_eq!(bucket2.err, Some("bucket does not exist".to_string()));
    }
}
