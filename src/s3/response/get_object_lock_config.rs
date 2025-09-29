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

use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasObject, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, ObjectLockConfig, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_object_lock_config`](crate::s3::client::MinioClient::get_object_lock_config) API call,
/// which retrieves the Object Lock configuration of a bucket.
///
/// This configuration determines the default retention mode and period applied to new objects,
/// helping to enforce write-once-read-many (WORM) protection.
#[derive(Clone, Debug)]
pub struct GetObjectLockConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetObjectLockConfigResponse);
impl_has_s3fields!(GetObjectLockConfigResponse);

impl HasBucket for GetObjectLockConfigResponse {}
impl HasRegion for GetObjectLockConfigResponse {}
impl HasObject for GetObjectLockConfigResponse {}

impl GetObjectLockConfigResponse {
    /// Returns the Object Lock configuration of the bucket.
    ///
    /// This method retrieves the Object Lock settings, which include retention mode and period,
    /// as well as legal hold status for the bucket.
    pub fn config(&self) -> Result<ObjectLockConfig, ValidationErr> {
        ObjectLockConfig::from_xml(&Element::parse(self.body.clone().reader())?)
    }
}
