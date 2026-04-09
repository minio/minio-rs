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
use crate::s3::error::{Error, ValidationErr};
use bytes::Bytes;
use http::HeaderMap;

#[derive(Debug, Clone)]
pub struct GetConfigResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(GetConfigResponse);
impl_has_madmin_fields!(GetConfigResponse);

impl GetConfigResponse {
    /// Returns the decrypted configuration data.
    pub fn config_data(&self) -> Result<Vec<u8>, Error> {
        let password = self
            .request
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for GetConfig response".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        crate::madmin::encrypt::decrypt_data(&password, &self.body)
    }
}
