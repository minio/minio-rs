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
use crate::madmin::types::service_account::ListAccessKeysResp;
use crate::s3::error::{Error, ValidationErr};
use bytes::Bytes;
use http::HeaderMap;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ListAccessKeysBulkResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ListAccessKeysBulkResponse);
impl_has_madmin_fields!(ListAccessKeysBulkResponse);

impl ListAccessKeysBulkResponse {
    /// Returns the decrypted access keys for all users.
    pub fn users_keys(&self) -> Result<HashMap<String, ListAccessKeysResp>, Error> {
        let password = self
            .request
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for decryption".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let decrypted_data = crate::madmin::encrypt::decrypt_data(&password, &self.body)?;

        serde_json::from_slice(&decrypted_data)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))
    }
}
