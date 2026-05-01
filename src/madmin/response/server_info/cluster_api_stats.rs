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

/// Response from the Cluster API Stats admin API
#[derive(Debug, Clone)]
pub struct ClusterAPIStatsResponse {
    pub stats: ClusterAPIStats,
}

/// Cluster API statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")] //TODO are these camelCase? rename_all still needed?
pub struct ClusterAPIStats {
    pub collected_at: DateTime<Utc>,
    pub nodes: u32,
    pub errors: Option<Vec<String>>,
    pub active_requests: u64,
    pub queued_requests: u64,
    pub last_minute: Option<APIMetrics>,
    pub last_day: Option<APIMetrics>,
    pub last_day_segmented: Option<Vec<APIMetrics>>,
}

/// API metrics for a time period
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct APIMetrics {
    #[serde(default)]
    pub total_requests: u64,
    #[serde(default)]
    pub total_errors: u64,
    #[serde(default)]
    pub total_5xx: u64,
    #[serde(default)]
    pub total_4xx: u64,
    #[serde(default)]
    pub avg_duration_ms: f64,
    #[serde(default)]
    pub max_duration_ms: f64,
    pub by_api: Option<serde_json::Value>,
}

#[async_trait]
impl FromMadminResponse for ClusterAPIStatsResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let body = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;

        let stats: ClusterAPIStats =
            serde_json::from_slice(&body).map_err(crate::s3::error::ValidationErr::JsonError)?;

        Ok(ClusterAPIStatsResponse { stats })
    }
}
