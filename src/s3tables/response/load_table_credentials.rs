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

//! Response type for LoadTableCredentials operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasTablesFields;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Response from LoadTableCredentials operation
///
/// Contains vended credentials for accessing table data files.
#[derive(Clone, Debug)]
pub struct LoadTableCredentialsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

/// Storage credential for accessing table data
#[derive(Clone, Debug, Deserialize)]
pub struct StorageCredential {
    /// Storage location prefix where this credential is relevant
    #[serde(default)]
    pub prefix: String,
    /// AWS access key ID
    #[serde(rename = "aws-access-key-id", default)]
    pub access_key_id: String,
    /// AWS secret access key
    #[serde(rename = "aws-secret-access-key", default)]
    pub secret_access_key: String,
    /// AWS session token for temporary credentials
    #[serde(rename = "aws-session-token", default)]
    pub session_token: Option<String>,
    /// Credential expiration timestamp (ISO 8601)
    #[serde(rename = "expiration-time", default)]
    pub expiration_time: Option<String>,
}

impl LoadTableCredentialsResponse {
    /// Returns the list of storage credentials for accessing table data
    pub fn storage_credentials(&self) -> Result<Vec<StorageCredential>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        let creds = json
            .get("storage-credentials")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();
        Ok(creds)
    }

    /// Returns additional configuration from the response
    pub fn config(&self) -> Result<std::collections::HashMap<String, String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("config")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl_has_tables_fields!(LoadTableCredentialsResponse);
impl_from_tables_response!(LoadTableCredentialsResponse);
