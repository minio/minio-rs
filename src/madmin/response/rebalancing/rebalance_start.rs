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
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

//TODO can we just have a id method on RebalanceStartResponse that lazily parses the body when needed?
#[derive(Debug, Clone, Deserialize)]
struct RebalanceIDResponse {
    id: String,
}

/// Response for [`RebalanceStart`](crate::madmin::builders::RebalanceStart) admin API operation.
#[derive(Debug, Clone)]
pub struct RebalanceStartResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(RebalanceStartResponse);
impl_has_madmin_fields!(RebalanceStartResponse);

impl RebalanceStartResponse {
    /// Returns the unique identifier for the rebalance operation.
    pub fn id(&self) -> Result<String, ValidationErr> {
        let text = String::from_utf8(self.body.to_vec()).map_err(|e| ValidationErr::StrError {
            message: format!("Invalid UTF-8 in response: {}", e),
            source: None,
        })?;
        let id_resp: RebalanceIDResponse =
            serde_json::from_str(&text).map_err(ValidationErr::JsonError)?;
        Ok(id_resp.id)
    }
}
