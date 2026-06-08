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

use crate::s3::error::ValidationErr;
use crate::s3::response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// A single MinIO inventory configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InventoryConfiguration {
    #[serde(rename = "bucket", default)]
    pub bucket: String,
    #[serde(rename = "id", default)]
    pub id: String,
    #[serde(rename = "user", default)]
    pub user: String,
    #[serde(rename = "yamlDef", default, skip_serializing_if = "String::is_empty")]
    pub yaml_def: String,
}

/// Result of listing MinIO inventory configurations.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InventoryListResult {
    #[serde(rename = "items", default)]
    pub items: Vec<InventoryConfiguration>,
    #[serde(
        rename = "nextContinuationToken",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub next_continuation_token: String,
}

/// Status of a MinIO inventory job.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InventoryJobStatus {
    #[serde(rename = "bucket", default)]
    pub bucket: String,
    #[serde(rename = "id", default)]
    pub id: String,
    #[serde(rename = "user", default)]
    pub user: String,
    #[serde(rename = "accessKey", default)]
    pub access_key: String,
    #[serde(rename = "schedule", default)]
    pub schedule: String,
    #[serde(rename = "state", default)]
    pub state: String,
    #[serde(
        rename = "nextScheduledTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub next_scheduled_time: Option<String>,
    #[serde(rename = "startTime", default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(rename = "endTime", default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(
        rename = "lastUpdate",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update: Option<String>,
    #[serde(rename = "scanned", default, skip_serializing_if = "String::is_empty")]
    pub scanned: String,
    #[serde(rename = "matched", default, skip_serializing_if = "String::is_empty")]
    pub matched: String,
    #[serde(rename = "scannedCount", default, skip_serializing_if = "is_zero")]
    pub scanned_count: u64,
    #[serde(rename = "matchedCount", default, skip_serializing_if = "is_zero")]
    pub matched_count: u64,
    #[serde(rename = "recordsWritten", default, skip_serializing_if = "is_zero")]
    pub records_written: u64,
    #[serde(rename = "outputFilesCount", default, skip_serializing_if = "is_zero")]
    pub output_files_count: u64,
    #[serde(
        rename = "executionTime",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub execution_time: String,
    #[serde(rename = "numStarts", default, skip_serializing_if = "is_zero")]
    pub num_starts: u64,
    #[serde(rename = "numErrors", default, skip_serializing_if = "is_zero")]
    pub num_errors: u64,
    #[serde(rename = "numLockLosses", default, skip_serializing_if = "is_zero")]
    pub num_lock_losses: u64,
    #[serde(
        rename = "manifestPath",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub manifest_path: String,
    #[serde(rename = "retryAttempts", default, skip_serializing_if = "is_zero")]
    pub retry_attempts: u64,
    #[serde(
        rename = "lastFailTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub last_fail_time: Option<String>,
    #[serde(
        rename = "lastFailErrors",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub last_fail_errors: Vec<String>,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// Response of the [`generate_inventory_config_yaml`](crate::s3::client::MinioClient::generate_inventory_config_yaml) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct GenerateInventoryConfigYamlResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GenerateInventoryConfigYamlResponse);
impl_has_s3fields!(GenerateInventoryConfigYamlResponse);

impl HasBucket for GenerateInventoryConfigYamlResponse {}
impl HasRegion for GenerateInventoryConfigYamlResponse {}

impl GenerateInventoryConfigYamlResponse {
    /// Returns the generated YAML inventory configuration template.
    pub fn yaml(&self) -> Result<&str, ValidationErr> {
        Ok(std::str::from_utf8(self.body())?)
    }
}

/// Response of the [`put_bucket_inventory_configuration`](crate::s3::client::MinioClient::put_bucket_inventory_configuration) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct PutBucketInventoryConfigurationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(PutBucketInventoryConfigurationResponse);
impl_has_s3fields!(PutBucketInventoryConfigurationResponse);

impl HasBucket for PutBucketInventoryConfigurationResponse {}
impl HasRegion for PutBucketInventoryConfigurationResponse {}

/// Response of the [`get_bucket_inventory_configuration`](crate::s3::client::MinioClient::get_bucket_inventory_configuration) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct GetBucketInventoryConfigurationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketInventoryConfigurationResponse);
impl_has_s3fields!(GetBucketInventoryConfigurationResponse);

impl HasBucket for GetBucketInventoryConfigurationResponse {}
impl HasRegion for GetBucketInventoryConfigurationResponse {}

impl GetBucketInventoryConfigurationResponse {
    /// Returns the inventory configuration parsed from the JSON response body.
    pub fn config(&self) -> Result<InventoryConfiguration, ValidationErr> {
        Ok(serde_json::from_slice(self.body())?)
    }
}

/// Response of the [`delete_bucket_inventory_configuration`](crate::s3::client::MinioClient::delete_bucket_inventory_configuration) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct DeleteBucketInventoryConfigurationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteBucketInventoryConfigurationResponse);
impl_has_s3fields!(DeleteBucketInventoryConfigurationResponse);

impl HasBucket for DeleteBucketInventoryConfigurationResponse {}
impl HasRegion for DeleteBucketInventoryConfigurationResponse {}

/// Response of the [`list_bucket_inventory_configurations`](crate::s3::client::MinioClient::list_bucket_inventory_configurations) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct ListBucketInventoryConfigurationsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(ListBucketInventoryConfigurationsResponse);
impl_has_s3fields!(ListBucketInventoryConfigurationsResponse);

impl HasBucket for ListBucketInventoryConfigurationsResponse {}
impl HasRegion for ListBucketInventoryConfigurationsResponse {}

impl ListBucketInventoryConfigurationsResponse {
    /// Returns the list result parsed from the JSON response body.
    pub fn result(&self) -> Result<InventoryListResult, ValidationErr> {
        Ok(serde_json::from_slice(self.body())?)
    }
}

/// Response of the [`get_bucket_inventory_job_status`](crate::s3::client::MinioClient::get_bucket_inventory_job_status) API call (MinIO extension).
#[derive(Clone, Debug)]
pub struct GetBucketInventoryJobStatusResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketInventoryJobStatusResponse);
impl_has_s3fields!(GetBucketInventoryJobStatusResponse);

impl HasBucket for GetBucketInventoryJobStatusResponse {}
impl HasRegion for GetBucketInventoryJobStatusResponse {}

impl GetBucketInventoryJobStatusResponse {
    /// Returns the inventory job status parsed from the JSON response body.
    pub fn status(&self) -> Result<InventoryJobStatus, ValidationErr> {
        Ok(serde_json::from_slice(self.body())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_configuration_round_trip() {
        let cfg = InventoryConfiguration {
            bucket: "mybucket".to_string(),
            id: "inv-1".to_string(),
            user: "minioadmin".to_string(),
            yaml_def: "id: inv-1\n".to_string(),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let parsed: InventoryConfiguration = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, parsed);
    }

    #[test]
    fn inventory_configuration_wire_format() {
        let json = r#"{"bucket":"b","id":"i","user":"u","yamlDef":"d"}"#;
        let cfg: InventoryConfiguration = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.bucket, "b");
        assert_eq!(cfg.id, "i");
        assert_eq!(cfg.user, "u");
        assert_eq!(cfg.yaml_def, "d");
    }

    #[test]
    fn inventory_configuration_omits_empty_yaml() {
        let cfg = InventoryConfiguration {
            bucket: "b".to_string(),
            id: "i".to_string(),
            user: "u".to_string(),
            yaml_def: String::new(),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(!json.contains("yamlDef"));
    }

    #[test]
    fn inventory_list_result_wire_format() {
        let json = r#"{
            "items":[
                {"bucket":"b","id":"i1","user":"u"},
                {"bucket":"b","id":"i2","user":"u"}
            ],
            "nextContinuationToken":"tok-2"
        }"#;
        let lr: InventoryListResult = serde_json::from_str(json).unwrap();
        assert_eq!(lr.items.len(), 2);
        assert_eq!(lr.items[0].id, "i1");
        assert_eq!(lr.next_continuation_token, "tok-2");
    }

    #[test]
    fn inventory_list_result_default_empty() {
        let lr: InventoryListResult = serde_json::from_str("{}").unwrap();
        assert!(lr.items.is_empty());
        assert_eq!(lr.next_continuation_token, "");
    }

    #[test]
    fn inventory_job_status_round_trip() {
        let status = InventoryJobStatus {
            bucket: "mybucket".to_string(),
            id: "inv-1".to_string(),
            user: "minioadmin".to_string(),
            access_key: "AKIA".to_string(),
            schedule: "@daily".to_string(),
            state: "running".to_string(),
            next_scheduled_time: Some("2026-01-01T00:00:00Z".to_string()),
            start_time: Some("2025-12-31T00:00:00Z".to_string()),
            end_time: None,
            last_update: Some("2025-12-31T01:00:00Z".to_string()),
            scanned: "100".to_string(),
            matched: "50".to_string(),
            scanned_count: 100,
            matched_count: 50,
            records_written: 50,
            output_files_count: 2,
            execution_time: "1h30m".to_string(),
            num_starts: 3,
            num_errors: 1,
            num_lock_losses: 0,
            manifest_path: "manifest.json".to_string(),
            retry_attempts: 2,
            last_fail_time: Some("2025-12-30T00:00:00Z".to_string()),
            last_fail_errors: vec!["disk full".to_string()],
        };
        let json = serde_json::to_string(&status).unwrap();
        let parsed: InventoryJobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, parsed);
    }

    #[test]
    fn inventory_job_status_wire_format() {
        let json = r#"{
            "bucket":"b",
            "id":"i",
            "user":"u",
            "accessKey":"ak",
            "schedule":"@daily",
            "state":"completed",
            "scannedCount":10,
            "matchedCount":5,
            "recordsWritten":5,
            "outputFilesCount":1,
            "executionTime":"2m",
            "retryAttempts":1,
            "lastFailErrors":["boom"]
        }"#;
        let s: InventoryJobStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.access_key, "ak");
        assert_eq!(s.scanned_count, 10);
        assert_eq!(s.matched_count, 5);
        assert_eq!(s.execution_time, "2m");
        assert_eq!(s.retry_attempts, 1);
        assert_eq!(s.last_fail_errors, vec!["boom".to_string()]);
        assert!(s.next_scheduled_time.is_none());
    }

    #[test]
    fn inventory_job_status_execution_time_is_string() {
        let json = r#"{"bucket":"b","id":"i","user":"u","accessKey":"ak","schedule":"s","state":"st","executionTime":"1h2m3s"}"#;
        let s: InventoryJobStatus = serde_json::from_str(json).unwrap();
        assert_eq!(s.execution_time, "1h2m3s");
    }
}
