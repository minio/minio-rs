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
use crate::madmin::types::MadminRequest;
use crate::madmin::types::lock::LockEntry;
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;

/// Response from the Top Locks admin API
#[derive(Debug, Clone)]
pub struct TopLocksResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(TopLocksResponse);
impl_has_madmin_fields!(TopLocksResponse);

impl TopLocksResponse {
    /// Returns the list of top locks.
    pub fn locks(&self) -> Result<Vec<LockEntry>, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(|e| ValidationErr::StrError {
            message: format!("Failed to parse TopLocks response: {e}"),
            source: None, //TODO shoule we wrap the original error here? Check this for all "source: None"
        })
    }
}
