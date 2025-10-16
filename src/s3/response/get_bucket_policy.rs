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
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Response from the [`get_bucket_policy`](crate::s3::client::MinioClient::get_bucket_policy) API call,
/// providing the bucket policy associated with an S3 bucket.
///
/// The bucket policy is a JSON-formatted string that defines permissions for the bucket,
/// specifying who can access the bucket and what actions they can perform.
///
/// For more information, refer to the [AWS S3 GetBucketPolicy API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html).
#[derive(Clone, Debug)]
pub struct GetBucketPolicyResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(GetBucketPolicyResponse);

impl HasBucket for GetBucketPolicyResponse {}
impl HasRegion for GetBucketPolicyResponse {}

impl GetBucketPolicyResponse {
    /// Returns the bucket policy as a JSON-formatted string.
    ///
    /// This method retrieves the policy associated with the bucket, which defines permissions
    /// for accessing the bucket and its contents.
    pub fn config(&self) -> Result<&str, ValidationErr> {
        Ok(std::str::from_utf8(&self.body)?)
    }
}

#[async_trait]
impl FromS3Response for GetBucketPolicyResponse {
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
                if matches!(e.code(), MinioErrorCode::NoSuchBucketPolicy) =>
            {
                Ok(Self {
                    request,
                    headers: e.take_headers(),
                    body: Bytes::from_static("{}".as_ref()),
                })
            }
            Err(e) => Err(e),
        }
    }
}
