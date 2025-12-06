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

//! Response type for FetchScanTasks operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response::plan_table_scan::{DeleteFile, FileScanTask};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Response from FetchScanTasks operation
#[derive(Clone, Debug)]
pub struct FetchScanTasksResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_tables_fields!(FetchScanTasksResponse);
impl_from_tables_response!(FetchScanTasksResponse);

/// Result of fetching scan tasks
#[derive(Clone, Debug, Deserialize)]
pub struct FetchScanTasksResult {
    #[serde(rename = "delete-files", default)]
    pub delete_files: Vec<DeleteFile>,
    #[serde(rename = "scan-tasks", default)]
    pub scan_tasks: Vec<FileScanTask>,
}

impl FetchScanTasksResponse {
    /// Parses the scan tasks result from the response body
    pub fn result(&self) -> Result<FetchScanTasksResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
