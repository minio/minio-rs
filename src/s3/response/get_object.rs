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
    pub bucket_name: String,
    pub object_name: String,
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
        let mut resp = resp?;
        let headers = mem::take(resp.headers_mut());
        let version_id = headers
            .get("x-amz-version-id")
            .map(|v| v.to_str().unwrap().to_string());

        let etag = headers
            .get("etag")
            .map(|v| v.to_str().unwrap().trim_matches('"').to_string());

        let content_length = resp.content_length().ok_or(Error::ContentLengthUnknown)?;
        let body = resp.bytes_stream().map_err(std::io::Error::other);

        let content = ObjectContent::new_from_stream(body, Some(content_length));

        Ok(GetObjectResponse {
            headers,
            region: req.region.unwrap_or("".to_string()),
            bucket_name: req.bucket.unwrap().to_string(),
            object_name: req.object.unwrap().to_string(),
            version_id,
            content,
            object_size: content_length,
            etag,
        })
    }
}
