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
use crate::s3::error::Error;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response from the Data Usage Info admin API
#[derive(Debug, Clone)]
pub struct DataUsageInfoResponse {
    pub info: DataUsageInfo,
}

/// Data usage information for the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataUsageInfo {
    #[serde(rename = "lastUpdate")]
    pub last_update: DateTime<Utc>,
    pub objects_count: u64,
    pub objects_total_size: u64,
    pub buckets_count: u64,
    #[serde(rename = "bucketsUsage")]
    pub buckets_usage: Option<HashMap<String, BucketUsageInfo>>,
    #[serde(rename = "tierStats")]
    pub tier_stats: Option<HashMap<String, TierStats>>,
    pub replication_pending_size: Option<u64>,
    pub replication_failed_size: Option<u64>,
    pub replicated_size: Option<u64>,
    pub replication_pending_count: Option<u64>,
    pub replication_failed_count: Option<u64>,
    pub replica_size: Option<u64>,
    pub replica_count: Option<u64>,
    pub capacity: Option<u64>,
    pub free_capacity: Option<u64>,
    pub used_capacity: Option<u64>,
}

/// Per-bucket usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketUsageInfo {
    pub size: u64,
    pub objects_count: u64,
    pub versioned_objects_count: Option<u64>,
    pub delete_markers_count: Option<u64>,
    pub replication_pending_size: Option<u64>,
    pub replication_failed_size: Option<u64>,
    pub replicated_size: Option<u64>,
    pub replication_pending_count: Option<u64>,
    pub replication_failed_count: Option<u64>,
    pub replica_size: Option<u64>,
    pub replica_count: Option<u64>,
}

/// Tier statistics for tiered storage
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TierStats {
    pub total_size: u64,
    pub num_versions: u64,
    pub num_objects: u64,
}

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for DataUsageInfoResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let body = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;

        let info: DataUsageInfo =
            serde_json::from_slice(&body).map_err(crate::s3::error::ValidationErr::JsonError)?;

        Ok(DataUsageInfoResponse { info })
    }
}
