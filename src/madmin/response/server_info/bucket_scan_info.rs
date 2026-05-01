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

use crate::impl_from_madmin_response;
use crate::impl_has_madmin_fields;
use crate::madmin::response::response_traits::HasBucket;
use crate::madmin::types::MadminRequest;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::HeaderMap;
use serde::{Deserialize, Serialize};

/// Response from the Bucket Scan Info admin API
#[derive(Clone, Debug)]
pub struct BucketScanInfoResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(BucketScanInfoResponse);
impl_has_madmin_fields!(BucketScanInfoResponse);
impl HasBucket for BucketScanInfoResponse {}

impl BucketScanInfoResponse {
    /// Returns the bucket scanning status information.
    pub fn scans(&self) -> Result<Vec<BucketScanInfo>, crate::s3::error::ValidationErr> {
        serde_json::from_slice(&self.body).map_err(crate::s3::error::ValidationErr::JsonError)
    }
}

/// Bucket scanning status information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketScanInfo {
    pub pool: i32,
    pub set: i32,
    pub cycle: u64,
    pub ongoing: bool,
    pub last_update: DateTime<Utc>,
    pub last_started: DateTime<Utc>,
    pub last_error: Option<String>,
    pub completed: Option<Vec<DateTime<Utc>>>,
}
