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

//! Response type for CreateNamespace operation

use crate::s3::error::Error;
use crate::s3::tables::types::{FromTablesResponse, TablesRequest};
use serde::Deserialize;
use std::collections::HashMap;

/// Response from CreateNamespace operation
#[derive(Debug, Clone, Deserialize)]
pub struct CreateNamespaceResponse {
    /// Namespace identifier (multi-level)
    pub namespace: Vec<String>,
    /// Namespace properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl FromTablesResponse for CreateNamespaceResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let response = request.execute().await?;
        let body = response
            .text()
            .await
            .map_err(crate::s3::error::NetworkError::ReqwestError)?;
        let result: CreateNamespaceResponse =
            serde_json::from_str(&body).map_err(crate::s3::error::ValidationErr::JsonError)?;
        Ok(result)
    }
}
