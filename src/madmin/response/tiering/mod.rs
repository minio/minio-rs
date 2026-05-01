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

pub use crate::madmin::types::tier::{TierConfig, TierInfo};

#[derive(Debug, Clone)]
pub struct AddTierResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(AddTierResponse);
impl_has_madmin_fields!(AddTierResponse);

#[derive(Debug, Clone)]
pub struct ListTiersResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ListTiersResponse);
impl_has_madmin_fields!(ListTiersResponse);

impl ListTiersResponse {
    /// Returns the list of tier configurations.
    pub fn tiers(&self) -> Result<Vec<TierConfig>, ValidationErr> {
        let text = std::str::from_utf8(&self.body).map_err(|e| ValidationErr::StrError {
            message: format!("Invalid UTF-8 in response: {}", e),
            source: Some(Box::new(e)),
        })?;
        serde_json::from_str::<Option<Vec<TierConfig>>>(text)
            .map_err(ValidationErr::JsonError)
            .map(|opt| opt.unwrap_or_default())
    }
}

#[derive(Debug, Clone)]
pub struct EditTierResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(EditTierResponse);
impl_has_madmin_fields!(EditTierResponse);

#[derive(Debug, Clone)]
pub struct RemoveTierResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(RemoveTierResponse);
impl_has_madmin_fields!(RemoveTierResponse);

#[derive(Debug, Clone)]
pub struct VerifyTierResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(VerifyTierResponse);
impl_has_madmin_fields!(VerifyTierResponse);

#[derive(Debug, Clone)]
pub struct TierStatsResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(TierStatsResponse);
impl_has_madmin_fields!(TierStatsResponse);

impl TierStatsResponse {
    /// Returns the tier statistics.
    pub fn stats(&self) -> Result<Vec<TierInfo>, ValidationErr> {
        let text = std::str::from_utf8(&self.body).map_err(|e| ValidationErr::StrError {
            message: format!("Invalid UTF-8 in response: {}", e),
            source: Some(Box::new(e)),
        })?;
        serde_json::from_str::<Option<Vec<TierInfo>>>(text)
            .map_err(ValidationErr::JsonError)
            .map(|opt| opt.unwrap_or_default())
    }
}
