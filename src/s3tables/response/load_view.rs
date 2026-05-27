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

//! Response type for LoadView operation
//!
//! # Specification
//!
//! Implements the response for `GET /v1/{prefix}/namespaces/{namespace}/views/{view}` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns the complete view metadata, including schema, view versions, history,
//! and properties.
//!
//! ## Response Schema (LoadViewResult)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `metadata-location` | `string` | Location of the view's metadata file |
//! | `metadata` | `ViewMetadata` | Complete view metadata |
//! | `config` | `object` or `null` | View-specific configuration properties |

use crate::impl_from_tables_response_with_cache;
use crate::impl_has_cached_view_result;
use crate::impl_has_tables_fields;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::collections::HashMap;

/// Response from LoadView operation
///
/// # Specification
///
/// Implements `GET /v1/{prefix}/namespaces/{namespace}/views/{view}` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`cached_view_result()`](crate::s3tables::HasCachedViewResult::cached_view_result) - Returns the complete view result
/// - [`view_metadata()`](crate::s3tables::HasCachedViewResult::view_metadata) - Returns the view metadata
/// - [`view_metadata_location()`](crate::s3tables::HasCachedViewResult::view_metadata_location) - Returns the metadata file location
/// - [`view_config()`](crate::s3tables::HasCachedViewResult::view_config) - Returns additional configuration properties
#[derive(Debug)]
pub struct LoadViewResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_result: OnceCell<LoadViewResult>,
}

/// View metadata from response
#[derive(Clone, Debug, Deserialize)]
pub struct ViewMetadata {
    #[serde(rename = "view-uuid")]
    pub view_uuid: String,
    #[serde(rename = "format-version")]
    pub format_version: i32,
    pub location: String,
    #[serde(rename = "current-version-id")]
    pub current_version_id: i32,
    pub versions: Vec<ViewVersion>,
    #[serde(rename = "version-log")]
    pub version_log: Vec<ViewHistoryEntry>,
    pub schemas: Vec<serde_json::Value>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// View version
#[derive(Clone, Debug, Deserialize)]
pub struct ViewVersion {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub summary: HashMap<String, String>,
    #[serde(rename = "default-namespace")]
    pub default_namespace: Vec<String>,
    #[serde(rename = "default-catalog")]
    pub default_catalog: Option<String>,
    pub representations: Vec<ViewRepresentation>,
}

/// View representation (SQL)
#[derive(Clone, Debug, Deserialize)]
pub struct ViewRepresentation {
    pub r#type: String,
    pub sql: String,
    pub dialect: String,
}

/// View history entry
#[derive(Clone, Debug, Deserialize)]
pub struct ViewHistoryEntry {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

/// Complete view result containing metadata, location, and config
#[derive(Clone, Debug, Deserialize)]
pub struct LoadViewResult {
    /// The view metadata
    pub metadata: ViewMetadata,
    /// Location of the metadata file
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    /// Additional configuration properties
    #[serde(default)]
    pub config: HashMap<String, String>,
}

impl_has_tables_fields!(LoadViewResponse);
impl_from_tables_response_with_cache!(LoadViewResponse);
impl_has_cached_view_result!(LoadViewResponse);
