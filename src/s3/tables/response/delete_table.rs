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

//! Response type for DeleteTable operation

use crate::s3::error::Error;
use crate::s3::tables::types::{FromTablesResponse, TablesRequest};

/// Response from DeleteTable operation
#[derive(Debug, Clone)]
pub struct DeleteTableResponse {}

impl FromTablesResponse for DeleteTableResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let _response = request.execute().await?;
        Ok(DeleteTableResponse {})
    }
}
