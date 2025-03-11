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

use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

/// Response of
/// [enable_object_legal_hold()](crate::s3::client::Client::enable_object_legal_hold)
/// API
#[derive(Clone, Debug)]
pub struct EnableObjectLegalHoldResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for EnableObjectLegalHoldResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };
        let mut resp = resp?;

        let object: String = req.object.unwrap();
        let version_id: Option<String> = req.query_params.get("versionId").cloned();

        Ok(EnableObjectLegalHoldResponse {
            headers: mem::take(resp.headers_mut()),
            region: req.inner_region,
            bucket,
            object,
            version_id,
        })
    }
}
