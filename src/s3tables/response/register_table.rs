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

//! Response type for RegisterTable operation
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces/{namespace}/register` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns the complete table metadata for the registered table. This operation registers
//! an existing table by providing its metadata file location.
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
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasTableResult;
use crate::s3tables::types::{LoadTableResult, TablesRequest};
use bytes::Bytes;
use http::HeaderMap;

/// Response from RegisterTable operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces/{namespace}/register` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`table_result()`](Self::table_result) - Returns the complete table result including metadata and location
#[derive(Clone, Debug)]
pub struct RegisterTableResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl RegisterTableResponse {}

impl_has_tables_fields!(RegisterTableResponse);
impl_from_tables_response!(RegisterTableResponse);
impl HasTableResult for RegisterTableResponse {
    fn table_result(&self) -> Result<LoadTableResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
