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

//! Response type for PlanTableScan operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Response from PlanTableScan operation
#[derive(Clone, Debug)]
pub struct PlanTableScanResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_tables_fields!(PlanTableScanResponse);
impl_from_tables_response!(PlanTableScanResponse);

/// Scan planning status
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlanningStatus {
    Completed,
    Submitted,
    Failed,
    Cancelled,
}

/// Result of a scan planning operation
#[derive(Clone, Debug, Deserialize)]
pub struct PlanTableScanResult {
    pub status: PlanningStatus,
    #[serde(rename = "plan-id")]
    pub plan_id: Option<String>,
    #[serde(rename = "plan-tasks", default)]
    pub plan_tasks: Vec<serde_json::Value>,
    #[serde(rename = "file-scan-tasks", default)]
    pub file_scan_tasks: Vec<FileScanTask>,
}

/// A file scan task returned from planning
#[derive(Clone, Debug, Deserialize)]
pub struct FileScanTask {
    #[serde(rename = "data-file")]
    pub data_file: Option<DataFile>,
    #[serde(rename = "delete-files", default)]
    pub delete_files: Vec<DeleteFile>,
    pub start: Option<i64>,
    pub length: Option<i64>,
    #[serde(rename = "spec-id")]
    pub spec_id: Option<i32>,
    #[serde(rename = "partition")]
    pub partition: Option<serde_json::Value>,
    pub residual: Option<serde_json::Value>,
}

/// Data file metadata
#[derive(Clone, Debug, Deserialize)]
pub struct DataFile {
    #[serde(rename = "file-path")]
    pub file_path: String,
    #[serde(rename = "file-format")]
    pub file_format: Option<String>,
    #[serde(rename = "record-count")]
    pub record_count: Option<i64>,
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: Option<i64>,
    #[serde(rename = "column-sizes")]
    pub column_sizes: Option<serde_json::Value>,
    #[serde(rename = "value-counts")]
    pub value_counts: Option<serde_json::Value>,
    #[serde(rename = "null-value-counts")]
    pub null_value_counts: Option<serde_json::Value>,
    #[serde(rename = "nan-value-counts")]
    pub nan_value_counts: Option<serde_json::Value>,
    #[serde(rename = "lower-bounds")]
    pub lower_bounds: Option<serde_json::Value>,
    #[serde(rename = "upper-bounds")]
    pub upper_bounds: Option<serde_json::Value>,
    #[serde(rename = "split-offsets")]
    pub split_offsets: Option<Vec<i64>>,
    // V3: Content type (DATA, POSITION_DELETES, EQUALITY_DELETES, or DELETION_VECTORS)
    pub content: Option<String>,
    // V3: Equality field IDs for equality delete files
    #[serde(rename = "equality-ids")]
    pub equality_ids: Option<Vec<i32>>,
    // V3: Sort order ID
    #[serde(rename = "sort-order-id")]
    pub sort_order_id: Option<i32>,
    // V3: First row ID for row lineage (when using _row_id)
    #[serde(rename = "first-row-id")]
    pub first_row_id: Option<i64>,
    // V3: Deletion vector reference (inline DV)
    #[serde(rename = "deletion-vector")]
    pub deletion_vector: Option<DeletionVectorRef>,
}

impl DataFile {
    /// Returns true if this data file has a deletion vector attached
    pub fn has_deletion_vector(&self) -> bool {
        self.deletion_vector.is_some()
    }

    /// Returns the content type (V3), defaults to "DATA" if not specified
    pub fn content_type(&self) -> &str {
        self.content.as_deref().unwrap_or("DATA")
    }
}

/// Reference to a deletion vector stored in a Puffin file (V3)
///
/// Deletion vectors in Iceberg V3 use roaring bitmaps to efficiently track
/// which row positions have been deleted within a data file.
#[derive(Clone, Debug, Deserialize)]
pub struct DeletionVectorRef {
    /// Path to the Puffin file containing the deletion vector blob
    #[serde(rename = "file-path")]
    pub file_path: String,
    /// Byte offset within the Puffin file where the DV blob starts
    pub offset: i64,
    /// Length of the DV blob in bytes
    pub length: i64,
    /// Cardinality (number of deleted rows)
    pub cardinality: Option<i64>,
}

/// Delete file metadata
#[derive(Clone, Debug, Deserialize)]
pub struct DeleteFile {
    #[serde(rename = "file-path")]
    pub file_path: String,
    #[serde(rename = "file-format")]
    pub file_format: Option<String>,
    #[serde(rename = "record-count")]
    pub record_count: Option<i64>,
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: Option<i64>,
    pub content: Option<String>,
}

impl PlanTableScanResponse {
    /// Parses the scan planning result from the response body
    pub fn result(&self) -> Result<PlanTableScanResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
