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
use crate::s3::error::{Error, S3ServerError, ValidationErr};
use crate::s3::minio_error_response::MinioErrorCode;
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields, HasTagging};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Response from the [`get_bucket_tagging`](crate::s3::client::MinioClient::get_bucket_tagging) API call,
/// providing the set of tags associated with an S3 bucket.
///
/// Tags are key-value pairs that help organize and manage resources,
/// often used for cost allocation and access control.
///
/// For more information, refer to the [AWS S3 GetBucketTagging API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html).
#[derive(Clone, Debug)]
pub struct GetBucketTaggingResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(GetBucketTaggingResponse);

impl HasBucket for GetBucketTaggingResponse {}
impl HasRegion for GetBucketTaggingResponse {}
impl HasTagging for GetBucketTaggingResponse {}

#[async_trait]
impl FromS3Response for GetBucketTaggingResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match response {
            Ok(mut resp) => Ok(Self {
                request,
                headers: mem::take(resp.headers_mut()),
                body: resp.bytes().await.map_err(ValidationErr::from)?,
            }),
            Err(Error::S3Server(S3ServerError::S3Error(mut e)))
                if matches!(e.code(), MinioErrorCode::NoSuchTagSet) =>
            {
                Ok(Self {
                    request,
                    headers: e.take_headers(),
                    body: Bytes::new(),
                })
            }
            Err(e) => Err(e),
        }
    }
}
