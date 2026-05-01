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

//! Response type for admin inventory control operations.

use crate::impl_from_madmin_response;
use crate::impl_has_madmin_fields;
use crate::madmin::response::inventory::types::AdminControlJson;
use crate::madmin::response::response_traits::{HasBucket, HasMadminFields};
use crate::madmin::types::MadminRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from admin inventory control operations (cancel/suspend/resume).
///
/// Confirms the action was performed successfully.
#[derive(Clone, Debug)]
pub struct AdminInventoryControlResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(AdminInventoryControlResponse);
impl_has_madmin_fields!(AdminInventoryControlResponse);

impl HasBucket for AdminInventoryControlResponse {}

impl AdminInventoryControlResponse {
    /// Parses the admin control JSON from the response body.
    pub fn admin_control(&self) -> Result<AdminControlJson, crate::s3::error::Error> {
        serde_json::from_slice(self.body()).map_err(|e| {
            crate::s3::error::Error::Validation(crate::s3::error::ValidationErr::InvalidConfig {
                message: format!("Failed to parse admin control JSON: {e}"),
            })
        })
    }
}
