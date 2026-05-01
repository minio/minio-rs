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

//! Response type for ReplaceView operation
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces/{namespace}/views/{view}` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns the complete view metadata after committing the changes. The response includes
//! the new metadata location and complete view metadata.
//!
//! ## Response Schema (LoadViewResult)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `metadata-location` | `string` | Location of the updated metadata file |
//! | `metadata` | `ViewMetadata` | Complete updated view metadata |
//! | `config` | `object` or `null` | View-specific configuration properties |

use crate::impl_from_tables_response_with_cache;
use crate::impl_has_cached_view_result;
use crate::impl_has_tables_fields;
use crate::s3tables::response::load_view::LoadViewResult;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from ReplaceView operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces/{namespace}/views/{view}` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`cached_view_result()`](crate::s3tables::HasCachedViewResult::cached_view_result) - Returns the complete view result
/// - [`view_metadata()`](crate::s3tables::HasCachedViewResult::view_metadata) - Returns the updated view metadata
/// - [`view_metadata_location()`](crate::s3tables::HasCachedViewResult::view_metadata_location) - Returns the new metadata file location
/// - [`view_config()`](crate::s3tables::HasCachedViewResult::view_config) - Returns additional configuration properties
#[derive(Debug)]
pub struct ReplaceViewResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_result: OnceCell<LoadViewResult>,
}

impl_has_tables_fields!(ReplaceViewResponse);
impl_from_tables_response_with_cache!(ReplaceViewResponse);
impl_has_cached_view_result!(ReplaceViewResponse);
