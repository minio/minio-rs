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
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`
//! from the [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns vended credentials for accessing the table's data files. These credentials
//! are scoped to the table's storage location and may be temporary.
//!
//! ## Response Schema (LoadCredentialsResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `storage-credentials` | `array[StorageCredential]` | List of storage credentials |
//! | `config` | `object` or `null` | Additional configuration properties |

use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::collections::HashMap;

/// Response from LoadTableCredentials operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` (HTTP 200 response)
/// from the [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`credentials_result()`](Self::credentials_result) - Returns the complete credentials result
/// - [`storage_credentials()`](Self::storage_credentials) - Returns the list of storage credentials
/// - [`config()`](Self::config) - Returns additional configuration as strings
/// - [`config_value()`](Self::config_value) - Returns additional configuration with preserved JSON types
#[derive(Debug)]
pub struct LoadTableCredentialsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_result: OnceCell<CredentialsResult>,
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

/// Complete credentials result containing storage credentials and config
#[derive(Clone, Debug, Deserialize)]
pub struct CredentialsResult {
    #[serde(rename = "storage-credentials")]
    pub storage_credentials: Vec<StorageCredential>,
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

impl LoadTableCredentialsResponse {
    fn get_or_parse(&self) -> Result<&CredentialsResult, ValidationErr> {
        self.cached_result
            .get_or_try_init(|| serde_json::from_slice(&self.body))
            .map_err(ValidationErr::JsonError)
    }

    /// Parses and returns the complete credentials result in a single deserialization
    pub fn credentials_result(&self) -> Result<&CredentialsResult, ValidationErr> {
        self.get_or_parse()
    }

    /// Returns the list of storage credentials for accessing table data
    pub fn storage_credentials(&self) -> Result<&[StorageCredential], ValidationErr> {
        Ok(&self.get_or_parse()?.storage_credentials)
    }

    /// Returns additional configuration from the response with preserved JSON types
    ///
    /// This method preserves the original JSON types (string, number, boolean, etc.)
    /// from the configuration object.
    pub fn config_value(&self) -> Result<&HashMap<String, serde_json::Value>, ValidationErr> {
        Ok(&self.get_or_parse()?.config)
    }

    /// Returns additional configuration from the response as strings (for backward compatibility)
    ///
    /// Prefer `config_value()` for preserving JSON types and accessing typed configuration values.
    /// This method converts all values to strings, which may lose type information.
    pub fn config(&self) -> Result<HashMap<String, String>, ValidationErr> {
        Ok(self
            .get_or_parse()?
            .config
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect())
    }
}

impl_has_tables_fields!(LoadTableCredentialsResponse);

#[async_trait::async_trait]
impl crate::s3tables::types::FromTablesResponse for LoadTableCredentialsResponse {
    async fn from_table_response(
        request: crate::s3tables::types::TablesRequest,
        response: Result<reqwest::Response, crate::s3::error::Error>,
    ) -> Result<Self, crate::s3::error::Error> {
        let mut resp = response?;
        Ok(Self {
            request,
            headers: std::mem::take(resp.headers_mut()),
            body: resp
                .bytes()
                .await
                .map_err(crate::s3::error::NetworkError::ReqwestError)?,
            cached_result: OnceCell::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_storage_credential_parsing() {
        let cred_json = json!({
            "prefix": "s3://bucket/path/",
            "aws-access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "aws-secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "aws-session-token": "FwoGZXIvYXdzEBaaDIVw...",
            "expiration-time": "2025-12-07T23:59:59Z"
        });

        let cred: StorageCredential =
            serde_json::from_value(cred_json).expect("Failed to parse credential");

        assert_eq!(cred.prefix, "s3://bucket/path/");
        assert_eq!(cred.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            cred.session_token,
            Some("FwoGZXIvYXdzEBaaDIVw...".to_string())
        );
        assert_eq!(
            cred.expiration_time,
            Some("2025-12-07T23:59:59Z".to_string())
        );
    }

    #[test]
    fn test_storage_credential_optional_fields() {
        let cred_json = json!({
            "prefix": "s3://bucket/",
            "aws-access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "aws-secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        });

        let cred: StorageCredential =
            serde_json::from_value(cred_json).expect("Failed to parse credential");

        assert_eq!(cred.prefix, "s3://bucket/");
        assert_eq!(cred.session_token, None);
        assert_eq!(cred.expiration_time, None);
    }

    #[test]
    fn test_multiple_storage_credentials() {
        let creds_json = json!([
            {
                "prefix": "s3://bucket/data/",
                "aws-access-key-id": "key1",
                "aws-secret-access-key": "secret1"
            },
            {
                "prefix": "s3://bucket/temp/",
                "aws-access-key-id": "key2",
                "aws-secret-access-key": "secret2"
            }
        ]);

        let creds: Vec<StorageCredential> =
            serde_json::from_value(creds_json).expect("Failed to parse credentials");

        assert_eq!(creds.len(), 2);
        assert_eq!(creds[0].access_key_id, "key1");
        assert_eq!(creds[1].access_key_id, "key2");
    }

    #[test]
    fn test_config_value_with_mixed_types() {
        let config_json = json!({
            "string_value": "test",
            "boolean_value": true,
            "number_value": 42,
            "float_value": 2.71
        });

        let config: HashMap<String, serde_json::Value> = config_json
            .as_object()
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        assert_eq!(config.len(), 4);
        assert_eq!(config["string_value"].as_str(), Some("test"));
        assert_eq!(config["boolean_value"].as_bool(), Some(true));
        assert_eq!(config["number_value"].as_i64(), Some(42));
        assert_eq!(config["float_value"].as_f64(), Some(2.71));
    }

    #[test]
    fn test_config_value_preserves_types() {
        let config_obj = json!({
            "enabled": true,
            "timeout": 30,
            "description": "Test config"
        });

        let config: HashMap<String, serde_json::Value> = config_obj
            .as_object()
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        // Verify types are preserved
        assert!(config["enabled"].is_boolean());
        assert!(config["timeout"].is_number());
        assert!(config["description"].is_string());
    }

    #[test]
    fn test_empty_config() {
        let config: HashMap<String, serde_json::Value> = HashMap::new();
        assert!(config.is_empty());
    }
}
