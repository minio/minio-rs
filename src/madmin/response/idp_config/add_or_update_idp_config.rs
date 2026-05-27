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

use crate::impl_has_madmin_fields;
use crate::madmin::headers::X_MINIO_RESTART;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

#[derive(Debug, Clone)]
pub struct AddOrUpdateIdpConfigResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_madmin_fields!(AddOrUpdateIdpConfigResponse);

impl AddOrUpdateIdpConfigResponse {
    /// Returns whether a server restart is required to apply the IDP config changes.
    pub fn restart_required(&self) -> bool {
        self.headers
            .get(X_MINIO_RESTART)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == "true")
            .unwrap_or(false)
    }
}

//TODO why is this function not replaced by the macro impl_from_madmin_response! ?

#[async_trait]
impl FromMadminResponse for AddOrUpdateIdpConfigResponse {
    async fn from_madmin_response(
        request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        Ok(AddOrUpdateIdpConfigResponse {
            request,
            headers: mem::take(resp.headers_mut()),
            body: resp.bytes().await.map_err(ValidationErr::HttpError)?,
        })
    }
}
