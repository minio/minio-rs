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
use crate::s3::utils::{take_bucket, take_object};
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

pub struct ObjectPromptResponse {
    /// Set of HTTP headers returned by the server.
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub prompt_response: String,
}

#[async_trait]
impl FromS3Response for ObjectPromptResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let prompt_response: String = String::from_utf8(body.to_vec())?;

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            prompt_response,
        })
    }
}
