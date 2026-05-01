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
use serde::{Deserialize, Serialize};

/// User information data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfoData {
    pub status: String,
    #[serde(rename = "policyName")]
    pub policy_name: Option<String>,
    #[serde(rename = "memberOf")]
    pub member_of: Option<Vec<String>>,
}

/// Response from the User Info admin API
#[derive(Debug, Clone)]
pub struct UserInfoResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(UserInfoResponse);
impl_has_madmin_fields!(UserInfoResponse);

impl UserInfoResponse {
    /// Returns the user information.
    pub fn user_info(&self) -> Result<UserInfoData, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
