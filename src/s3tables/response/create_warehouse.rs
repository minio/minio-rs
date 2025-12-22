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

//! Response type for CreateWarehouse operation
//!
//! # Specification
//!
//! Implements the response for creating a warehouse. This is a MinIO-specific extension
//! to the Iceberg REST Catalog API for managing S3 Tables warehouses.
//!
//! ## Response (HTTP 200)
//!
//! Returns the created warehouse details including name, UUID, bucket, and creation time.
//!
//! ## Response Schema
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `warehouse` | `string` | The warehouse name |
//! | `uuid` | `string` | Unique identifier for the warehouse |
//! | `bucket` | `string` | S3 bucket associated with the warehouse |
//! | `created_at` | `string` | ISO 8601 timestamp of creation |

use crate::s3tables::response_traits::{HasBucket, HasCreatedAt, HasUuid, HasWarehouseName};
use crate::s3tables::types::TablesRequest;
use crate::{impl_from_tables_response_cached, impl_has_cached_body, impl_has_tables_fields};
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from CreateWarehouse operation
///
/// # Specification
///
/// Creates a new warehouse (MinIO-specific extension to Iceberg REST Catalog API).
///
/// # Available Fields
///
/// - [`warehouse_name()`](crate::s3tables::HasWarehouseName::warehouse_name) - Returns the warehouse name
/// - [`uuid()`](crate::s3tables::HasUuid::uuid) - Returns the warehouse UUID
/// - [`bucket()`](crate::s3tables::HasBucket::bucket) - Returns the associated S3 bucket
/// - [`created_at()`](crate::s3tables::HasCreatedAt::created_at) - Returns the creation timestamp
#[derive(Debug)]
pub struct CreateWarehouseResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(CreateWarehouseResponse);
impl_from_tables_response_cached!(CreateWarehouseResponse);
impl_has_cached_body!(CreateWarehouseResponse);

impl HasWarehouseName for CreateWarehouseResponse {}
impl HasBucket for CreateWarehouseResponse {}
impl HasUuid for CreateWarehouseResponse {}
impl HasCreatedAt for CreateWarehouseResponse {}
