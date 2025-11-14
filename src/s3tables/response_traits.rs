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

//! Trait composition for Tables API responses
//!
//! Provides common trait implementations for accessing response metadata similar to S3 responses.

use crate::s3::error::ValidationErr;
use crate::s3tables::iceberg::TableMetadata;
use crate::s3tables::types::{LoadTableResult, TablesRequest};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::HeaderMap;
use std::collections::HashMap;

#[macro_export]
/// Implements the `FromTablesResponse` trait for the specified types.
///
/// This macro generates the boilerplate code for parsing a Tables API response,
/// storing the request, headers, and body in the response struct.
macro_rules! impl_from_tables_response {
    ($($ty:ty),* $(,)?) => {
        $(
            #[async_trait::async_trait]
            impl $crate::s3tables::types::FromTablesResponse for $ty {
                async fn from_table_response(
                    request: $crate::s3tables::types::TablesRequest,
                    response: Result<reqwest::Response, $crate::s3::error::Error>,
                ) -> Result<Self, $crate::s3::error::Error> {
                    let mut resp = response?;
                    Ok(Self {
                        request,
                        headers: std::mem::take(resp.headers_mut()),
                        body: resp
                            .bytes()
                            .await
                            .map_err($crate::s3::error::NetworkError::ReqwestError)?,
                    })
                }
            }
        )*
    };
}

#[macro_export]
/// Implements the `HasTablesFields` trait for the specified types.
macro_rules! impl_has_tables_fields {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::s3tables::response_traits::HasTablesFields for $ty {
                /// The request that was sent to the Tables API.
                #[inline]
                fn request(&self) -> &$crate::s3tables::types::TablesRequest {
                    &self.request
                }

                /// HTTP headers returned by the server, containing metadata such as `Content-Type`, etc.
                #[inline]
                fn headers(&self) -> &http::HeaderMap {
                    &self.headers
                }

                /// The response body returned by the server, as raw bytes.
                #[inline]
                fn body(&self) -> &bytes::Bytes {
                    &self.body
                }
            }
        )*
    };
}

/// Base trait providing access to common response fields
///
/// Similar to `HasS3Fields` in the S3 API, this provides access to:
/// - The original request
/// - HTTP response headers
/// - Raw response body
///
/// All Tables response types should implement this trait.
pub trait HasTablesFields {
    /// The request that was sent to the Tables API.
    fn request(&self) -> &TablesRequest;
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, etc.
    fn headers(&self) -> &HeaderMap;
    /// The response body returned by the server, as raw bytes.
    fn body(&self) -> &Bytes;
}

/// Returns the warehouse name from the response body.
pub trait HasWarehouseName: HasTablesFields {
    /// Returns the warehouse name from the response body.
    #[inline]
    fn warehouse_name(&self) -> Result<String, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'name' field in response".into(),
                source: None,
            })
    }
}

/// Provides access to namespace name from response
///
/// Similar to `HasBucket` in S3 API. Typically used by namespace-related operations.
pub trait HasNamespace: HasTablesFields {
    /// Returns the namespace name from the response, or empty string if not found.
    ///
    /// Extracts from the response body which typically contains the namespace identifier
    /// as a JSON array: `{"namespace": ["name1", "name2"]}`
    /// Returns the first element joined with "."
    fn namespace(&self) -> &str {
        match serde_json::from_slice::<serde_json::Value>(self.body()) {
            Ok(json) => {
                if let Some(ns_array) = json.get("namespace").and_then(|v| v.as_array()) {
                    let parts: Vec<String> = ns_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    if !parts.is_empty() {
                        return Box::leak(parts.join(".").into_boxed_str());
                    }
                }
                ""
            }
            Err(_) => "",
        }
    }
}

pub trait HasNamespacesResponse: HasTablesFields {
    fn namespaces_from_result(&self) -> Result<Vec<String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("namespace")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'namespace' field in GetNamespace response".into(),
                source: None,
            })
    }
}

/// Returns the underlying S3 bucket name
pub trait HasBucket: HasTablesFields {
    /// Returns the underlying S3 bucket name
    fn bucket(&self) -> Result<String, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("bucket")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'bucket' field in CreateWarehouse response".into(),
                source: None,
            })
    }
}

/// Returns the unique identifier for the warehouse
pub trait HasUuid: HasTablesFields {
    /// Returns the unique identifier for the warehouse
    fn uuid(&self) -> Result<String, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("uuid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'uuid' field in CreateWarehouse response".into(),
                source: None,
            })
    }
}

pub trait HasCreatedAt: HasTablesFields {
    /// Returns the creation timestamp
    fn created_at(&self) -> Result<DateTime<Utc>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("created-at")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<DateTime<Utc>>().ok())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'created-at' field in response".into(),
                source: None,
            })
    }
}

/// Provides namespace properties from response
///
/// Convenience trait for accessing namespace properties.
pub trait HasProperties: HasTablesFields {
    /// Returns the namespace properties/metadata
    fn properties(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("properties")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// Provides table result information from response
///
/// Typically used by operations that return loaded table information like
/// CreateTable, LoadTable, and RegisterTable.
pub trait HasTableResult: HasTablesFields {
    /// Returns the table result containing metadata and location information
    fn table_result(&self) -> Result<LoadTableResult, ValidationErr> {
        serde_json::from_slice(self.body()).map_err(ValidationErr::JsonError)
    }
}

/// Provides table metadata information from response
///
/// Typically used by operations that commit table metadata like CommitTable.
/// These operations return Apache Iceberg table metadata updates.
pub trait HasTableMetadata: HasTablesFields {
    /// Returns the updated table metadata
    fn metadata(&self) -> Result<TableMetadata, ValidationErr>;

    /// Returns the location of the new metadata file
    fn metadata_location(&self) -> Result<String, ValidationErr>;
}

/// Provides pagination support for list operations
///
/// Typically used by list operations like ListWarehouses, ListNamespaces, and ListTables.
/// These operations support pagination through continuation tokens.
pub trait HasPagination: HasTablesFields {
    /// Returns the pagination token for fetching the next page, if available
    fn next_token(&self) -> Result<Option<String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("next-page-token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()))
    }
}
