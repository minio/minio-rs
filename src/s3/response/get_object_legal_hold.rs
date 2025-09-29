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
use crate::s3::response::a_response_traits::{
    HasBucket, HasObject, HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::get_text_default;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [get_object_legal_hold()](crate::s3::client::MinioClient::get_object_legal_hold)
/// API
#[derive(Clone, Debug)]
pub struct GetObjectLegalHoldResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetObjectLegalHoldResponse);
impl_has_s3fields!(GetObjectLegalHoldResponse);

impl HasBucket for GetObjectLegalHoldResponse {}
impl HasRegion for GetObjectLegalHoldResponse {}
impl HasObject for GetObjectLegalHoldResponse {}
impl HasVersion for GetObjectLegalHoldResponse {}

impl GetObjectLegalHoldResponse {
    /// Returns the legal hold status of the object.
    ///
    /// This method retrieves whether the legal hold is enabled for the specified object.
    pub fn enabled(&self) -> Result<bool, ValidationErr> {
        if self.body.is_empty() {
            return Ok(false); // No legal hold configuration present due to NoSuchObjectLockConfiguration
        }
        let root = Element::parse(self.body.clone().reader())?;
        Ok(get_text_default(&root, "Status") == "ON")
    }
}
