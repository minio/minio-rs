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

use crate::s3::error::{Error, ErrorCode};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::take_bucket;
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

/// Response from the [`get_bucket_policy`](crate::s3::client::Client::get_bucket_policy) API call,
/// providing the bucket policy associated with an S3 bucket.
///
/// The bucket policy is a JSON-formatted string that defines permissions for the bucket,
/// specifying who can access the bucket and what actions they can perform.
///
/// For more information, refer to the [AWS S3 GetBucketPolicy API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html).
#[derive(Clone, Debug)]
pub struct GetBucketPolicyResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose policy is retrieved.
    pub bucket: String,

    /// The bucket policy as a JSON-formatted string.
    ///
    /// This policy defines access permissions for the bucket. It specifies who can access the bucket,
    /// what actions they can perform, and under what conditions.
    ///
    /// For example, a policy might grant read-only access to anonymous users or restrict access to specific IP addresses.
    ///
    /// Note: If the bucket has no policy, the `get_bucket_policy` API call may return an error
    /// with the code `NoSuchBucketPolicy`. It's advisable to handle this case appropriately in your application.
    pub config: String,
}

#[async_trait]
impl FromS3Response for GetBucketPolicyResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => Ok(Self {
                headers: mem::take(r.headers_mut()),
                region: req.inner_region,
                bucket: take_bucket(req.bucket)?,
                config: r.text().await?,
            }),
            Err(Error::S3Error(e)) if e.code == ErrorCode::NoSuchBucketPolicy => Ok(Self {
                headers: e.headers,
                region: req.inner_region,
                bucket: take_bucket(req.bucket)?,
                config: String::from("{}"),
            }),
            Err(e) => Err(e),
        }
    }
}
