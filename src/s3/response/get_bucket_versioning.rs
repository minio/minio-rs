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

use crate::s3::builders::VersioningStatus;
use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{get_option_text, take_bucket};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_versioning`](crate::s3::client::Client::get_bucket_versioning) API call,
/// providing the versioning configuration of a bucket.
///
/// This includes the current versioning status and the MFA (Multi-Factor Authentication) delete setting,
/// if configured.
///
/// For more information, refer to the [AWS S3 GetBucketVersioning API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html).
#[derive(Clone, Debug)]
pub struct GetBucketVersioningResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose versioning configuration is retrieved.
    pub bucket: String,

    /// The versioning status of the bucket.
    ///
    /// - `Some(VersioningStatus::Enabled)`: Versioning is enabled.
    /// - `Some(VersioningStatus::Suspended)`: Versioning is suspended.
    /// - `None`: Versioning has never been configured for this bucket.
    pub status: Option<VersioningStatus>,

    /// Indicates whether MFA delete is enabled for the bucket.
    ///
    /// - `Some(true)`: MFA delete is enabled.
    /// - `Some(false)`: MFA delete is disabled.
    /// - `None`: MFA delete has never been configured for this bucket.
    ///
    /// Note: MFA delete adds an extra layer of security by requiring additional authentication
    /// for certain operations. For more details, see the [AWS S3 MFA Delete documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/MultiFactorAuthenticationDelete.html).
    pub mfa_delete: Option<bool>,
}

#[async_trait]
impl FromS3Response for GetBucketVersioningResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;
        let status: Option<VersioningStatus> =
            get_option_text(&root, "Status").map(|v| match v.as_str() {
                "Enabled" => VersioningStatus::Enabled,
                _ => VersioningStatus::Suspended, // Default case
            });
        let mfa_delete: Option<bool> =
            get_option_text(&root, "MFADelete").map(|v| v.eq_ignore_ascii_case("Enabled"));

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            status,
            mfa_delete,
        })
    }
}
