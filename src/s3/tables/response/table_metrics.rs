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

use crate::s3::error::Error;
use crate::s3::tables::types::{FromTablesResponse, TablesRequest};
use serde::Deserialize;

/// Response from TableMetrics operation
#[derive(Debug, Clone, Deserialize)]
pub struct TableMetricsResponse {
    /// Total number of rows in the table
    pub row_count: i64,
    /// Total size of the table in bytes
    pub size_bytes: i64,
    /// Number of data files
    pub file_count: i64,
    /// Number of snapshots
    pub snapshot_count: i64,
}

impl FromTablesResponse for TableMetricsResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let response = request.execute().await?;
        let body = response
            .text()
            .await
            .map_err(crate::s3::error::NetworkError::ReqwestError)?;
        let result: TableMetricsResponse =
            serde_json::from_str(&body).map_err(crate::s3::error::ValidationErr::JsonError)?;
        Ok(result)
    }
}
