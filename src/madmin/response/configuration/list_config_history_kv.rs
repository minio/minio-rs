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
use crate::madmin::types::config::ConfigHistoryEntry;
use crate::s3::error::{Error, ValidationErr};
use bytes::Bytes;
use http::HeaderMap;

/// Response for the ListConfigHistoryKV operation
///
/// Contains a list of configuration history entries sorted by creation time
#[derive(Debug, Clone)]
pub struct ListConfigHistoryKVResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ListConfigHistoryKVResponse);
impl_has_madmin_fields!(ListConfigHistoryKVResponse);

impl ListConfigHistoryKVResponse {
    /// Returns the list of configuration history entries.
    ///
    /// The response body is encrypted and will be decrypted using admin credentials.
    pub fn entries(&self) -> Result<Vec<ConfigHistoryEntry>, Error> {
        // Decrypt the response using admin credentials
        let password: String = self
            .request
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for ListConfigHistoryKV response".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let decrypted_data = crate::madmin::encrypt::decrypt_data(&password, &self.body)?;

        let entries: Vec<ConfigHistoryEntry> =
            serde_json::from_slice::<Option<Vec<ConfigHistoryEntry>>>(&decrypted_data)
                .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?
                .unwrap_or_default();

        Ok(entries)
    }
}
