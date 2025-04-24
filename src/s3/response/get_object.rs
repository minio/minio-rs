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

use crate::s3::utils::{take_bucket, take_object};
use crate::s3::{
    builders::ObjectContent,
    error::Error,
    types::{FromS3Response, S3Request},
};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use http::HeaderMap;
use std::mem;

pub struct GetObjectResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// Entity tag representing a specific version of the object.
    pub etag: Option<String>,

    /// Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,

    /// The content of the object as a stream or byte buffer.
    pub content: ObjectContent,

    /// Size of the object in bytes.
    pub object_size: u64,
}

#[async_trait]
impl FromS3Response for GetObjectResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

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

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            version_id,
            content,
            object_size: content_length,
            etag,
        })
    }
}
