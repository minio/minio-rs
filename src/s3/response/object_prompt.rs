// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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

pub struct ObjectPromptResponse {
    pub headers: http::HeaderMap,
    pub region: String,
    pub bucket_name: String,
    pub object_name: String,
    pub prompt_response: String,
}

#[async_trait]
impl FromS3Response for ObjectPromptResponse {
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        response: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        let prompt_response: String = String::from_utf8(body.to_vec())?;
        let region: String = req.region.unwrap_or("").to_string(); // Keep this since it defaults to an empty string

        let bucket_name: String = req
            .bucket
            .ok_or_else(|| {
                Error::InvalidBucketName(String::from("Missing bucket name in request"))
            })?
            .to_string();

        let object_name: String = req
            .object
            .ok_or_else(|| {
                Error::InvalidObjectName(String::from("Missing object name in request"))
            })?
            .to_string();

        Ok(ObjectPromptResponse {
            headers,
            region,
            bucket_name,
            object_name,
            prompt_response,
        })
    }
}
