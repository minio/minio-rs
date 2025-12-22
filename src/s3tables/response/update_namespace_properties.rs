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

//! Response type for UpdateNamespaceProperties operation
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces/{namespace}/properties` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns a list of the keys that were added, updated, or removed. Properties that were
//! requested for removal but did not exist are returned in the `missing` array.
//!
//! ## Response Schema (UpdateNamespacePropertiesResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `updated` | `array[string]` | List of property keys that were added or updated |
//! | `removed` | `array[string]` | List of property keys that were removed |
//! | `missing` | `array[string]` | List of property keys that were requested for removal but did not exist |

use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

/// Represents a property operation result in UpdateNamespaceProperties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyOperation {
    /// Property key that was operated on
    pub key: String,
}

impl PropertyOperation {
    /// Create a new property operation result
    pub fn new(key: String) -> Self {
        Self { key }
    }
}

/// Parsed namespace properties update result
#[derive(Debug, Clone, Deserialize, Default)]
pub struct PropertiesUpdateResult {
    #[serde(default)]
    pub updated: Vec<String>,
    #[serde(default)]
    pub removed: Vec<String>,
    #[serde(default)]
    pub missing: Vec<String>,
}

/// Response from UpdateNamespaceProperties operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces/{namespace}/properties` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`result()`](Self::result) - Returns all property update results in a single parsed structure
/// - [`updated()`](Self::updated) - Returns list of property keys that were added or updated
/// - [`removed()`](Self::removed) - Returns list of property keys that were removed
/// - [`missing()`](Self::missing) - Returns list of property keys requested for removal but did not exist
#[derive(Debug)]
pub struct UpdateNamespacePropertiesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_result: OnceCell<PropertiesUpdateResult>,
}

impl UpdateNamespacePropertiesResponse {
    fn get_or_parse(&self) -> Result<&PropertiesUpdateResult, ValidationErr> {
        self.cached_result
            .get_or_try_init(|| serde_json::from_slice(&self.body))
            .map_err(ValidationErr::JsonError)
    }

    /// Parses and returns all property update results in a single deserialization
    pub fn result(&self) -> Result<&PropertiesUpdateResult, ValidationErr> {
        self.get_or_parse()
    }

    /// Returns the list of property operations that were updated
    pub fn updated_operations(&self) -> Result<Vec<PropertyOperation>, ValidationErr> {
        Ok(self
            .get_or_parse()?
            .updated
            .iter()
            .cloned()
            .map(PropertyOperation::new)
            .collect())
    }

    /// Returns the list of property keys that were updated (for backward compatibility)
    ///
    /// Prefer `updated_operations()` for accessing structured property operation results.
    pub fn updated(&self) -> Result<&[String], ValidationErr> {
        Ok(&self.get_or_parse()?.updated)
    }

    /// Returns the list of property operations that were removed
    pub fn removed_operations(&self) -> Result<Vec<PropertyOperation>, ValidationErr> {
        Ok(self
            .get_or_parse()?
            .removed
            .iter()
            .cloned()
            .map(PropertyOperation::new)
            .collect())
    }

    /// Returns the list of property keys that were removed (for backward compatibility)
    ///
    /// Prefer `removed_operations()` for accessing structured property operation results.
    pub fn removed(&self) -> Result<&[String], ValidationErr> {
        Ok(&self.get_or_parse()?.removed)
    }

    /// Returns the list of property operations that were requested for removal but did not exist
    pub fn missing_operations(&self) -> Result<Vec<PropertyOperation>, ValidationErr> {
        Ok(self
            .get_or_parse()?
            .missing
            .iter()
            .cloned()
            .map(PropertyOperation::new)
            .collect())
    }

    /// Returns the list of property keys that were requested for removal but did not exist (for backward compatibility)
    ///
    /// Prefer `missing_operations()` for accessing structured property operation results.
    pub fn missing(&self) -> Result<&[String], ValidationErr> {
        Ok(&self.get_or_parse()?.missing)
    }
}

impl_has_tables_fields!(UpdateNamespacePropertiesResponse);

#[async_trait::async_trait]
impl crate::s3tables::types::FromTablesResponse for UpdateNamespacePropertiesResponse {
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

    #[test]
    fn test_property_operation_creation() {
        let op = PropertyOperation::new("test_key".to_string());
        assert_eq!(op.key, "test_key");
    }

    #[test]
    fn test_property_operation_equality() {
        let op1 = PropertyOperation::new("key1".to_string());
        let op2 = PropertyOperation::new("key1".to_string());
        let op3 = PropertyOperation::new("key2".to_string());

        assert_eq!(op1, op2);
        assert_ne!(op1, op3);
    }

    #[test]
    fn test_property_operations_parsing() {
        let operations: Vec<PropertyOperation> = vec![
            PropertyOperation::new("prop1".to_string()),
            PropertyOperation::new("prop2".to_string()),
        ];

        assert_eq!(operations.len(), 2);
        assert_eq!(operations[0].key, "prop1");
        assert_eq!(operations[1].key, "prop2");
    }

    #[test]
    fn test_property_operation_extraction_from_strings() {
        let keys = vec!["key1".to_string(), "key2".to_string()];
        let ops: Vec<PropertyOperation> = keys.into_iter().map(PropertyOperation::new).collect();

        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].key, "key1");
        assert_eq!(ops[1].key, "key2");
    }
}
