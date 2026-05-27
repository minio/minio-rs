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

//! Response type for CreateNamespace operation
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns the namespace created, as well as any properties that were stored for the namespace,
//! including those the server might have added (such as `last_modified_time`).
//!
//! ## Response Schema (CreateNamespaceResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `namespace` | `array[string]` | Reference to a namespace, e.g., `["accounting", "tax"]` |
//! | `properties` | `object` | Properties stored on the namespace (may include server-added properties) |

use crate::s3tables::response_traits::{HasNamespace, HasProperties};
use crate::s3tables::types::TablesRequest;
use crate::{impl_from_tables_response_cached, impl_has_cached_body, impl_has_tables_fields};
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from CreateNamespace operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`namespace()`](crate::s3tables::HasNamespace::namespace) - Returns the namespace identifier joined with "."
/// - [`namespace_parts()`](crate::s3tables::HasNamespace::namespace_parts) - Returns the namespace as array of parts
/// - [`properties()`](crate::s3tables::HasProperties::properties) - Returns namespace properties (may include server-added properties)
#[derive(Debug)]
pub struct CreateNamespaceResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(CreateNamespaceResponse);
impl_from_tables_response_cached!(CreateNamespaceResponse);
impl_has_cached_body!(CreateNamespaceResponse);

impl HasNamespace for CreateNamespaceResponse {}
impl HasProperties for CreateNamespaceResponse {}
