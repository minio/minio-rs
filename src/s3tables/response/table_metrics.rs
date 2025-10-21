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

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from TableMetrics operation
///
/// Follows the lazy evaluation pattern: stores raw response data and parses fields on demand.
#[derive(Clone, Debug)]
pub struct TableMetricsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl TableMetricsResponse {
    /// Returns the total number of rows in the table
    pub fn row_count(&self) -> Result<i64, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("row_count")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'row_count' field in TableMetrics response".into(),
                source: None,
            })
    }

    /// Returns the total size of the table in bytes
    pub fn size_bytes(&self) -> Result<i64, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("size_bytes")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'size_bytes' field in TableMetrics response".into(),
                source: None,
            })
    }

    /// Returns the number of data files
    pub fn file_count(&self) -> Result<i64, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("file_count")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'file_count' field in TableMetrics response".into(),
                source: None,
            })
    }

    /// Returns the number of snapshots
    pub fn snapshot_count(&self) -> Result<i64, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("snapshot_count")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'snapshot_count' field in TableMetrics response"
                    .into(),
                source: None,
            })
    }
}

impl_has_tables_fields!(TableMetricsResponse);
impl_from_tables_response!(TableMetricsResponse);
