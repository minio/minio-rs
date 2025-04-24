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
use crate::s3::multimap::MultimapExt;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{take_bucket, take_object};
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

/// Response from the [`enable_object_legal_hold`](crate::s3::client::Client::enable_object_legal_hold) API call,
/// indicating that a legal hold has been successfully applied to a specific object version in an S3 bucket.
///
/// Applying a legal hold prevents the object version from being overwritten or deleted until the legal hold is explicitly removed.
/// Legal holds are independent of retention periods and remain in effect until removed, regardless of any set retention expiration.
///
/// For more information, refer to the [AWS S3 PutObjectLegalHold API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html).
#[derive(Clone, Debug)]
pub struct EnableObjectLegalHoldResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (name) identifying the object within the bucket.
    pub object: String,

    /// The version ID of the object to which the legal hold was applied.
    ///
    /// If versioning is not enabled on the bucket, this field may be `None`.
    pub version_id: Option<String>,
}

#[async_trait]
impl FromS3Response for EnableObjectLegalHoldResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        Ok(Self {
            headers: mem::take(resp.headers_mut()),
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            version_id: req.query_params.take_version(),
        })
    }
}
