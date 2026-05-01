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
use crate::madmin::encrypt::decrypt_data;
use crate::madmin::types::MadminRequest;
use crate::s3::error::{Error, ValidationErr};
use bytes::Bytes;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// User information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub status: String,
    #[serde(rename = "policyName")]
    pub policy_name: Option<String>,
    #[serde(rename = "memberOf")]
    pub member_of: Option<Vec<String>>,
}

/// Response from the List Users admin API
#[derive(Debug, Clone)]
pub struct ListUsersResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ListUsersResponse);
impl_has_madmin_fields!(ListUsersResponse);

impl ListUsersResponse {
    /// Returns the decrypted list of users.
    pub fn users(&self) -> Result<HashMap<String, UserInfo>, Error> {
        let secret_key = self
            .request
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "No credentials provider available for decryption".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let decrypted = decrypt_data(&secret_key, &self.body)?;

        serde_json::from_slice(&decrypted)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))
    }
}
