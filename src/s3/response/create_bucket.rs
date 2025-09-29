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

use crate::impl_has_s3fields;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Response of
/// [create_bucket()](crate::s3::client::MinioClient::create_bucket)
/// API
#[derive(Clone, Debug)]
pub struct CreateBucketResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(CreateBucketResponse);

impl HasBucket for CreateBucketResponse {}
impl HasRegion for CreateBucketResponse {}

#[async_trait]
impl FromS3Response for CreateBucketResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp: reqwest::Response = response?;

        let mut request = request;
        let bucket = request
            .bucket
            .as_deref()
            .ok_or(ValidationErr::MissingBucketName)?;
        let region: &str = &request.inner_region;
        request.client.add_bucket_region(bucket, region);

        Ok(Self {
            request,
            headers: mem::take(resp.headers_mut()),
            body: resp.bytes().await.map_err(ValidationErr::from)?,
        })
    }
}
