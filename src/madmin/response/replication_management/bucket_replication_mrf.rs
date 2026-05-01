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
use crate::madmin::types::replication::ReplicationMRF;
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;

/// Response for [`BucketReplicationMRF`](crate::madmin::builders::BucketReplicationMRF) admin API operation.
///
/// Contains MRF (Metadata Replication Framework) backlog entries for objects that failed to replicate.
#[derive(Clone, Debug)]
pub struct BucketReplicationMRFResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(BucketReplicationMRFResponse);
impl_has_madmin_fields!(BucketReplicationMRFResponse);
impl HasBucket for BucketReplicationMRFResponse {}

impl BucketReplicationMRFResponse {
    /// Returns the list of MRF backlog entries.
    pub fn entries(&self) -> Result<Vec<ReplicationMRF>, ValidationErr> {
        let text = String::from_utf8(self.body.to_vec()).map_err(|e| ValidationErr::StrError {
            message: format!("Invalid UTF-8 in response: {}", e),
            source: Some(Box::new(e)),
        })?;

        let mut entries = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<ReplicationMRF>(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    eprintln!("Failed to parse MRF entry: {}", e);
                }
            }
        }
        Ok(entries)
    }
}
