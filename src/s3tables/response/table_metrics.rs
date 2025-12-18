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

//! Response type for TableMetrics operation
//!
//! # Specification
//!
//! Implements the response for `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns table metrics including row count, data size, file count, and snapshot count.
//!
//! ## Response Schema (TableMetrics)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `row_count` | `i64` | Total number of rows in the table |
//! | `size_bytes` | `i64` | Total size of the table in bytes |
//! | `file_count` | `i64` | Number of data files |
//! | `snapshot_count` | `i64` | Number of snapshots |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Parsed table metrics data
#[derive(Debug, Clone, Deserialize)]
struct TableMetrics {
    row_count: i64,
    size_bytes: i64,
    file_count: i64,
    snapshot_count: i64,
}

/// Response from TableMetrics operation
///
/// # Specification
///
/// Implements `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`row_count()`](Self::row_count) - Returns the total number of rows
/// - [`size_bytes()`](Self::size_bytes) - Returns the total size in bytes
/// - [`file_count()`](Self::file_count) - Returns the number of data files
/// - [`snapshot_count()`](Self::snapshot_count) - Returns the number of snapshots
#[derive(Clone, Debug)]
pub struct TableMetricsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl TableMetricsResponse {
    /// Parses and returns all metrics in a single deserialization
    fn metrics(&self) -> Result<TableMetrics, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }

    /// Returns the total number of rows in the table
    pub fn row_count(&self) -> Result<i64, ValidationErr> {
        Ok(self.metrics()?.row_count)
    }

    /// Returns the total size of the table in bytes
    pub fn size_bytes(&self) -> Result<i64, ValidationErr> {
        Ok(self.metrics()?.size_bytes)
    }

    /// Returns the number of data files
    pub fn file_count(&self) -> Result<i64, ValidationErr> {
        Ok(self.metrics()?.file_count)
    }

    /// Returns the number of snapshots
    pub fn snapshot_count(&self) -> Result<i64, ValidationErr> {
        Ok(self.metrics()?.snapshot_count)
    }
}

impl_has_tables_fields!(TableMetricsResponse);
impl_from_tables_response!(TableMetricsResponse);
