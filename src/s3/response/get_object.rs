// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

use crate::s3::{
    builders::ObjectContent,
    error::Error,
    types::{FromS3Response, S3Request},
};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use std::mem;

pub struct GetObjectResponse {
    pub headers: http::HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub content: ObjectContent,
    pub object_size: u64,
    pub etag: Option<String>,
}

#[async_trait]
impl FromS3Response for GetObjectResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket = req
            .bucket
            .ok_or_else(|| Error::InvalidBucketName("no bucket specified".into()))?;
        let object = req
            .object
            .ok_or_else(|| Error::InvalidObjectName("no object specified".into()))?;
        let mut resp = resp?;
        let headers = mem::take(resp.headers_mut());

        let etag: Option<String> = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string());

        let version_id: Option<String> = headers
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok().map(String::from));

        let content_length: u64 = resp.content_length().ok_or(Error::ContentLengthUnknown)?;
        let body = resp.bytes_stream().map_err(std::io::Error::other);
        let content = ObjectContent::new_from_stream(body, Some(content_length));

        Ok(GetObjectResponse {
            headers,
            region: req.region.unwrap_or("".to_string()),
            bucket,
            object,
            version_id,
            content,
            object_size: content_length,
            etag,
        })
    }
}
