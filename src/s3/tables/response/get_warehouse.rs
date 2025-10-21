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

//! Response type for GetWarehouse operation

use crate::s3::error::Error;
use crate::s3::tables::types::{FromTablesResponse, TablesRequest};
use chrono::{DateTime, Utc};
use serde::Deserialize;

/// Response from GetWarehouse operation
#[derive(Debug, Clone, Deserialize)]
pub struct GetWarehouseResponse {
    /// Name of the warehouse
    pub name: String,
    /// ARN of the warehouse
    pub arn: String,
    /// Creation timestamp
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp
    #[serde(rename = "lastModifiedAt")]
    pub last_modified_at: DateTime<Utc>,
}

impl FromTablesResponse for GetWarehouseResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let response = request.execute().await?;
        let body = response
            .text()
            .await
            .map_err(crate::s3::error::NetworkError::ReqwestError)?;
        let result: GetWarehouseResponse =
            serde_json::from_str(&body).map_err(crate::s3::error::ValidationErr::JsonError)?;
        Ok(result)
    }
}
