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

//! Response type for FetchPlanningResult operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response::plan_table_scan::{FileScanTask, PlanningStatus};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Response from FetchPlanningResult operation
#[derive(Clone, Debug)]
pub struct FetchPlanningResultResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_tables_fields!(FetchPlanningResultResponse);
impl_from_tables_response!(FetchPlanningResultResponse);

/// Result of fetching a planning result
#[derive(Clone, Debug, Deserialize)]
pub struct FetchPlanningResultData {
    pub status: PlanningStatus,
    #[serde(rename = "plan-tasks", default)]
    pub plan_tasks: Vec<serde_json::Value>,
    #[serde(rename = "file-scan-tasks", default)]
    pub file_scan_tasks: Vec<FileScanTask>,
}

impl FetchPlanningResultResponse {
    /// Parses the planning result from the response body
    pub fn result(&self) -> Result<FetchPlanningResultData, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
