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
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;
use serde::{Deserialize, Serialize};

/// Response from site replication peer bucket operations
#[derive(Clone, Debug)]
pub struct SiteReplicationPeerBucketOpsResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(SiteReplicationPeerBucketOpsResponse);
impl_has_madmin_fields!(SiteReplicationPeerBucketOpsResponse);
impl HasBucket for SiteReplicationPeerBucketOpsResponse {}

impl SiteReplicationPeerBucketOpsResponse {
    /// Returns the status of the operation.
    pub fn status(&self) -> Result<String, ValidationErr> {
        let parsed: SiteReplicationPeerBucketOpsParsed =
            serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)?;
        Ok(parsed.status)
    }

    /// Returns the error detail if present.
    pub fn err_detail(&self) -> Result<Option<String>, ValidationErr> {
        let parsed: SiteReplicationPeerBucketOpsParsed =
            serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)?;
        Ok(parsed.err_detail)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SiteReplicationPeerBucketOpsParsed {
    //TODO make status an enum? what does the go sdk do?
    pub status: String,
    #[serde(rename = "errDetail", skip_serializing_if = "Option::is_none")]
    pub err_detail: Option<String>,
}
