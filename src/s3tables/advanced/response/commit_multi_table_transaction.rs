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

//! Response type for CommitMultiTableTransaction operation
//!
//! # Specification
//!
//! Implements the response for committing changes to multiple tables atomically. This is part
//! of the Apache Iceberg REST Catalog API for transactional catalog operations.
//!
//! ## Response (HTTP 204)
//!
//! Returns no content on successful commit. All table updates in the transaction are
//! applied atomically - either all succeed or none do.
//!
//! ## Response Schema
//!
//! Empty body (HTTP 204 No Content).

use crate::impl_from_tables_response_cached;
use crate::impl_has_cached_body;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::HasWarehouseName;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from CommitMultiTableTransaction operation
///
/// # Specification
///
/// Commits changes to multiple tables atomically.
///
/// # Note
///
/// This response contains an empty body (HTTP 204 No Content). The trait implementations
/// are provided for API consistency but the accessor methods will fail since there is
/// no JSON body to parse. The successful return of this response indicates all table
/// updates in the transaction were committed successfully.
#[derive(Debug)]
pub struct CommitMultiTableTransactionResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(CommitMultiTableTransactionResponse);
impl_from_tables_response_cached!(CommitMultiTableTransactionResponse);
impl_has_cached_body!(CommitMultiTableTransactionResponse);

impl HasWarehouseName for CommitMultiTableTransactionResponse {}
