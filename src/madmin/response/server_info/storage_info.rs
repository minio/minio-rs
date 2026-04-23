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

use crate::madmin::types::storage::StorageInfo;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct StorageInfoResponse(pub StorageInfo);

impl Deref for StorageInfoResponse {
    type Target = StorageInfo;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for StorageInfoResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let body = resp.bytes().await.map_err(ValidationErr::HttpError)?;

        let storage_info: StorageInfo = serde_json::from_slice(&body)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        Ok(StorageInfoResponse(storage_info))
    }
}
