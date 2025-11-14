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

use crate::impl_has_s3fields;
use crate::s3::builders::ObjectContent;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response_traits::{HasBucket, HasEtagFromHeaders, HasObject, HasRegion, HasVersion};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use http::HeaderMap;
use std::mem;

pub struct GetObjectResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes, // Note: not used
    resp: reqwest::Response,
}

impl_has_s3fields!(GetObjectResponse);

impl HasBucket for GetObjectResponse {}
impl HasRegion for GetObjectResponse {}
impl HasObject for GetObjectResponse {}
impl HasVersion for GetObjectResponse {}
impl HasEtagFromHeaders for GetObjectResponse {}

impl GetObjectResponse {
    /// Returns the content of the object as a (streaming) byte buffer. Note: consumes the response.
    pub fn content(self) -> Result<ObjectContent, Error> {
        let content_length: u64 = self.object_size()?;
        let body = self.resp.bytes_stream().map_err(std::io::Error::other);
        Ok(ObjectContent::new_from_stream(body, Some(content_length)))
    }

    /// Returns the content size (in Bytes) of the object.
    pub fn object_size(&self) -> Result<u64, ValidationErr> {
        self.resp
            .content_length()
            .ok_or(ValidationErr::ContentLengthUnknown)
    }
}

#[async_trait]
impl FromS3Response for GetObjectResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        Ok(Self {
            request,
            headers: mem::take(resp.headers_mut()),
            body: Bytes::new(),
            resp,
        })
    }
}
