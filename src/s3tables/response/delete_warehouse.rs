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

//! Response type for DeleteWarehouse operation
//!
//! # Specification
//!
//! Implements the response for deleting a warehouse. This is a MinIO-specific extension
//! to the Iceberg REST Catalog API for managing S3 Tables warehouses.
//!
//! ## Response (HTTP 204)
//!
//! Returns no content on successful deletion.
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

/// Response from DeleteWarehouse operation
///
/// # Specification
///
/// Deletes a warehouse (MinIO-specific extension to Iceberg REST Catalog API).
///
/// # Note
///
/// This response contains an empty body (HTTP 204 No Content). The trait implementations
/// are provided for API consistency but the accessor methods will fail since there is
/// no JSON body to parse. The successful return of this response indicates the warehouse
/// was deleted.
#[derive(Debug)]
pub struct DeleteWarehouseResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(DeleteWarehouseResponse);
impl_from_tables_response_cached!(DeleteWarehouseResponse);
impl_has_cached_body!(DeleteWarehouseResponse);

impl HasWarehouseName for DeleteWarehouseResponse {}
