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

//! Response type for DropView operation
//!
//! # Specification
//!
//! Implements the response for `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 204)
//!
//! Returns no content on successful deletion.
//!
//! ## Response Schema
//!
//! Empty body (HTTP 204 No Content).

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from DropView operation
///
/// # Specification
///
/// Implements `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}` (HTTP 204 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Note
///
/// This response contains an empty body (HTTP 204 No Content). The successful return
/// of this response indicates the view was deleted.
#[derive(Clone, Debug)]
pub struct DropViewResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_tables_fields!(DropViewResponse);
impl_from_tables_response!(DropViewResponse);
