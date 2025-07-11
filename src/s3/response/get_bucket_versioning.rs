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
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::get_text_option;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_versioning`](crate::s3::client::MinioClient::get_bucket_versioning) API call,
/// providing the versioning configuration of a bucket.
///
/// This includes the current versioning status and the MFA (Multi-Factor Authentication) delete setting,
/// if configured.
///
/// For more information, refer to the [AWS S3 GetBucketVersioning API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html).
#[derive(Clone, Debug)]
pub struct GetBucketVersioningResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketVersioningResponse);
impl_has_s3fields!(GetBucketVersioningResponse);

impl HasBucket for GetBucketVersioningResponse {}
impl HasRegion for GetBucketVersioningResponse {}

impl GetBucketVersioningResponse {
    /// Returns the versioning status of the bucket.
    ///
    /// This method retrieves the current versioning status, which can be:
    /// - `Some(VersioningStatus::Enabled)` if versioning is enabled.
    /// - `Some(VersioningStatus::Suspended)` if versioning is suspended.
    /// - `None` if versioning has never been configured for this bucket.
    pub fn status(&self) -> Result<Option<VersioningStatus>, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;
        Ok(get_text_option(&root, "Status").map(|v| match v.as_str() {
            "Enabled" => VersioningStatus::Enabled,
            _ => VersioningStatus::Suspended, // Default case
        }))
    }

    /// Returns whether MFA delete is enabled for the bucket.
    ///
    /// This method retrieves the MFA delete setting, which can be:
    /// - `Some(true)` if MFA delete is enabled.
    /// - `Some(false)` if MFA delete is disabled.
    /// - `None` if MFA delete has never been configured for this bucket.
    pub fn mfa_delete(&self) -> Result<Option<bool>, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;
        Ok(get_text_option(&root, "MFADelete").map(|v| v.eq_ignore_ascii_case("Enabled")))
    }
}
