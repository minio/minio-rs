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

use crate::s3::client::DEFAULT_REGION;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [get_region()](crate::s3::client::MinioClient::get_region)
/// API
#[derive(Clone, Debug)]
pub struct GetRegionResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetRegionResponse);
impl_has_s3fields!(GetRegionResponse);

impl HasBucket for GetRegionResponse {}
impl HasRegion for GetRegionResponse {}

impl GetRegionResponse {
    /// Returns the region response for the bucket.
    ///
    /// This method retrieves the region where the bucket is located.
    pub fn region_response(&self) -> Result<String, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;

        let mut location = root.get_text().unwrap_or_default().to_string();
        if location.is_empty() {
            location = String::from(DEFAULT_REGION);
        }
        Ok(location)
    }
}
