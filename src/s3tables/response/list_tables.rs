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

//! Response type for ListTables operation
//!
//! # Specification
//!
//! Implements the response for `GET /v1/{prefix}/namespaces/{namespace}/tables` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns a list of table identifiers within the namespace. If the catalog supports pagination,
//! the response will include a `next-page-token` for fetching the next page of results.
//!
//! ## Response Schema (ListTablesResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `identifiers` | `array[TableIdentifier]` | List of table identifiers (namespace + name) |
//! | `next-page-token` | `string` or `null` | Token for pagination (optional) |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasPagination;
use crate::s3tables::types::{TableIdentifier, TablesRequest};
use bytes::Bytes;
use http::HeaderMap;

/// Response from ListTables operation
///
/// # Specification
///
/// Implements `GET /v1/{prefix}/namespaces/{namespace}/tables` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`identifiers()`](Self::identifiers) - Returns the list of table identifiers
/// - [`next_token()`](crate::s3tables::HasPagination::next_token) - Returns pagination token for next page (if any)
#[derive(Clone, Debug)]
pub struct ListTablesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl ListTablesResponse {
    /// Returns the list of table identifiers
    pub fn identifiers(&self) -> Result<Vec<TableIdentifier>, ValidationErr> {
        #[derive(serde::Deserialize)]
        struct IdentifiersWrapper {
            identifiers: Vec<TableIdentifier>,
        }

        serde_json::from_slice::<IdentifiersWrapper>(&self.body)
            .map(|wrapper| wrapper.identifiers)
            .map_err(ValidationErr::JsonError)
    }
}

impl_has_tables_fields!(ListTablesResponse);
impl_from_tables_response!(ListTablesResponse);
impl HasPagination for ListTablesResponse {}
