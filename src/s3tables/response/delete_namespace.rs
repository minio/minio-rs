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

//! Response type for DeleteNamespace operation
//!
//! # Specification
//!
//! Implements the response for `DELETE /v1/{prefix}/namespaces/{namespace}` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 204)
//!
//! Returns no content on successful deletion. The namespace must be empty (no tables)
//! for deletion to succeed.
//!
//! ## Response Schema
//!
//! Empty body (HTTP 204 No Content).

use crate::impl_from_tables_response_cached;
use crate::impl_has_cached_body;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::{HasNamespace, HasWarehouseName};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from DeleteNamespace operation
///
/// # Specification
///
/// Implements `DELETE /v1/{prefix}/namespaces/{namespace}` (HTTP 204 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Note
///
/// This response contains an empty body (HTTP 204 No Content). The trait implementations
/// are provided for API consistency but the accessor methods will fail since there is
/// no JSON body to parse. The successful return of this response indicates the namespace
/// was deleted.
#[derive(Debug)]
pub struct DeleteNamespaceResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(DeleteNamespaceResponse);
impl_from_tables_response_cached!(DeleteNamespaceResponse);
impl_has_cached_body!(DeleteNamespaceResponse);

impl HasNamespace for DeleteNamespaceResponse {}
impl HasWarehouseName for DeleteNamespaceResponse {}
