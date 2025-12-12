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
//!
//! # Specification
//!
//! Response structures follow the [Apache Iceberg REST Catalog API specification](https://iceberg.apache.org/spec/#rest-catalog-api).
//! The OpenAPI specification is available at:
//! <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>

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
///
/// Note: For types that need cached body parsing, use `impl_from_tables_response_cached!` instead.
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
/// Implements the `FromTablesResponse` trait for types with cached body parsing.
///
/// This macro generates the boilerplate code for parsing a Tables API response,
/// storing the request, headers, body, and initializing the cache in the response struct.
macro_rules! impl_from_tables_response_cached {
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
                        cached_body: once_cell::sync::OnceCell::new(),
                    })
                }
            }
        )*
    };
}

#[macro_export]
/// Implements the `FromTablesResponse` trait for types with a custom cached result field.
///
/// This macro generates the boilerplate code for parsing a Tables API response,
/// storing the request, headers, body, and initializing the custom cache field.
/// Use this for types like `LoadViewResponse` that cache a specific result type
/// rather than the generic `serde_json::Value`.
macro_rules! impl_from_tables_response_with_cache {
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
                        cached_result: once_cell::sync::OnceCell::new(),
                    })
                }
            }
        )*
    };
}

#[macro_export]
/// Implements the `HasCachedViewResult` trait for types with a `cached_result` field
/// containing a `LoadViewResult`.
macro_rules! impl_has_cached_view_result {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::s3tables::response_traits::HasCachedViewResult for $ty {
                fn cached_view_result(
                    &self,
                ) -> Result<&$crate::s3tables::response::load_view::LoadViewResult, $crate::s3::error::ValidationErr> {
                    self.cached_result
                        .get_or_try_init(|| serde_json::from_slice(&self.body))
                        .map_err($crate::s3::error::ValidationErr::JsonError)
                }
            }
        )*
    };
}

#[macro_export]
/// Implements the `HasCachedBody` trait for types with a `cached_body` field.
macro_rules! impl_has_cached_body {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::s3tables::response_traits::HasCachedBody for $ty {
                fn cached_body(&self) -> Result<&serde_json::Value, $crate::s3::error::ValidationErr> {
                    self.cached_body
                        .get_or_try_init(|| serde_json::from_slice(&self.body))
                        .map_err($crate::s3::error::ValidationErr::JsonError)
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

/// Trait for responses that cache their parsed JSON body.
///
/// This trait enables efficient access to response data by parsing the JSON body
/// only once and caching the result. All traits that need to extract fields from
/// the JSON body should use this trait as a supertrait.
pub trait HasCachedBody: HasTablesFields {
    /// Returns a reference to the cached parsed JSON body.
    ///
    /// The body is parsed on first access and cached for subsequent calls.
    fn cached_body(&self) -> Result<&serde_json::Value, ValidationErr>;
}

/// Returns the warehouse name from the response body.
pub trait HasWarehouseName: HasCachedBody {
    /// Returns the warehouse name from the response body.
    #[inline]
    fn warehouse_name(&self) -> Result<String, ValidationErr> {
        let json = self.cached_body()?;
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
pub trait HasNamespace: HasCachedBody {
    /// Returns the namespace name from the response.
    ///
    /// Extracts from the response body which typically contains the namespace identifier
    /// as a JSON array: `{"namespace": ["name1", "name2"]}`
    /// Returns the elements joined with "."
    fn namespace(&self) -> Result<String, ValidationErr> {
        let json = self.cached_body()?;
        let ns_array = json
            .get("namespace")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'namespace' field in response".into(),
                source: None,
            })?;

        let parts: Vec<&str> = ns_array.iter().filter_map(|v| v.as_str()).collect();
        if parts.is_empty() {
            return Err(ValidationErr::StrError {
                message: "Empty namespace in response".into(),
                source: None,
            });
        }
        Ok(parts.join("."))
    }

    /// Returns the namespace as a list of parts
    fn namespace_parts(&self) -> Result<Vec<String>, ValidationErr> {
        let json = self.cached_body()?;
        json.get("namespace")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'namespace' field in response".into(),
                source: None,
            })
    }
}

pub trait HasNamespacesResponse: HasCachedBody {
    fn namespaces_from_result(&self) -> Result<Vec<String>, ValidationErr> {
        let json = self.cached_body()?;
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
pub trait HasBucket: HasCachedBody {
    /// Returns the underlying S3 bucket name
    fn bucket(&self) -> Result<String, ValidationErr> {
        let json = self.cached_body()?;
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
pub trait HasUuid: HasCachedBody {
    /// Returns the unique identifier for the warehouse
    fn uuid(&self) -> Result<String, ValidationErr> {
        let json = self.cached_body()?;
        json.get("uuid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'uuid' field in CreateWarehouse response".into(),
                source: None,
            })
    }
}

pub trait HasCreatedAt: HasCachedBody {
    /// Returns the creation timestamp
    fn created_at(&self) -> Result<DateTime<Utc>, ValidationErr> {
        let json = self.cached_body()?;
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
pub trait HasProperties: HasCachedBody {
    /// Returns the namespace properties/metadata
    fn properties(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let json = self.cached_body()?;
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

/// Trait for responses that cache their parsed view result.
///
/// This trait enables efficient access to view data by parsing the JSON body
/// once into a strongly-typed `LoadViewResult` and caching it.
pub trait HasCachedViewResult: HasTablesFields {
    /// Returns a reference to the cached parsed view result.
    ///
    /// The body is parsed on first access and cached for subsequent calls.
    fn cached_view_result(
        &self,
    ) -> Result<&crate::s3tables::response::load_view::LoadViewResult, ValidationErr>;

    /// Returns the view metadata
    fn view_metadata(
        &self,
    ) -> Result<&crate::s3tables::response::load_view::ViewMetadata, ValidationErr> {
        Ok(&self.cached_view_result()?.metadata)
    }

    /// Returns the metadata location
    fn view_metadata_location(&self) -> Result<&str, ValidationErr> {
        Ok(&self.cached_view_result()?.metadata_location)
    }

    /// Returns additional config from the response
    fn view_config(&self) -> Result<&std::collections::HashMap<String, String>, ValidationErr> {
        Ok(&self.cached_view_result()?.config)
    }
}

/// Provides pagination support for list operations
///
/// Typically used by list operations like ListWarehouses, ListNamespaces, and ListTables.
/// These operations support pagination through continuation tokens.
pub trait HasPagination: HasTablesFields {
    /// Returns the pagination token for fetching the next page, if available
    fn next_token(
        &self,
    ) -> Result<Option<crate::s3tables::types::ContinuationToken>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("next-page-token")
            .and_then(|v| v.as_str())
            .map(crate::s3tables::types::ContinuationToken::new))
    }
}
