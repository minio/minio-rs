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

//! Response type for ListViews operation
//!
//! # Specification
//!
//! Implements the response for `GET /v1/{prefix}/namespaces/{namespace}/views` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns a list of view identifiers within the namespace. If the catalog supports pagination,
//! the response will include a `next-page-token` for fetching the next page of results.
//!
//! ## Response Schema (ListTablesResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `identifiers` | `array[TableIdentifier]` | List of view identifiers (namespace + name) |
//! | `next-page-token` | `string` or `null` | Token for pagination (optional) |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::{HasPagination, HasTablesFields};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Response from ListViews operation
///
/// # Specification
///
/// Implements `GET /v1/{prefix}/namespaces/{namespace}/views` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`identifiers()`](Self::identifiers) - Returns the list of view identifiers
/// - [`next_token()`](crate::s3tables::HasPagination::next_token) - Returns pagination token for next page (if any)
#[derive(Clone, Debug)]
pub struct ListViewsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

/// View identifier in list response
#[derive(Clone, Debug, Deserialize)]
pub struct ViewIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

impl ListViewsResponse {
    /// Returns the list of view identifiers
    pub fn identifiers(&self) -> Result<Vec<ViewIdentifier>, ValidationErr> {
        #[derive(serde::Deserialize)]
        struct ViewsWrapper {
            #[serde(default)]
            identifiers: Option<Vec<ViewIdentifier>>,
        }

        serde_json::from_slice::<ViewsWrapper>(self.body())
            .map(|wrapper| wrapper.identifiers.unwrap_or_default())
            .map_err(ValidationErr::JsonError)
    }
}

impl_has_tables_fields!(ListViewsResponse);
impl_from_tables_response!(ListViewsResponse);
impl HasPagination for ListViewsResponse {}
