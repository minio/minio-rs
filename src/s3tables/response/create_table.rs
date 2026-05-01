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

//! Response type for CreateTable operation
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces/{namespace}/tables` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns the complete table metadata for the newly created table, including the
//! metadata location and any server-assigned properties.
//!
//! ## Response Schema (LoadTableResult)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `metadata-location` | `string` | Location of the table's metadata file |
//! | `metadata` | `TableMetadata` | Complete table metadata |
//! | `config` | `object` or `null` | Table-specific configuration properties |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::HasTableResult;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from CreateTable operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces/{namespace}/tables` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`table_result()`](crate::s3tables::HasTableResult::table_result) - Returns the complete table result
/// - [`metadata()`](crate::s3tables::HasTableMetadata::metadata) - Returns the table metadata
/// - [`metadata_location()`](crate::s3tables::HasTableMetadata::metadata_location) - Returns the metadata file location
#[derive(Clone, Debug)]
pub struct CreateTableResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl CreateTableResponse {}

impl_has_tables_fields!(CreateTableResponse);
impl_from_tables_response!(CreateTableResponse);
impl HasTableResult for CreateTableResponse {}
