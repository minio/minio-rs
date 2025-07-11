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
use crate::s3::response::a_response_traits::HasS3Fields;
use crate::s3::types::{Bucket, FromS3Response, S3Request};
use crate::s3::utils::{from_iso8601utc, get_text_result};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of [list_buckets()](crate::s3::client::MinioClient::list_buckets) API
#[derive(Debug, Clone)]
pub struct ListBucketsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(ListBucketsResponse);
impl_has_s3fields!(ListBucketsResponse);

impl ListBucketsResponse {
    /// Returns the list of buckets in the account.
    pub fn buckets(&self) -> Result<Vec<Bucket>, ValidationErr> {
        let mut root = Element::parse(self.body().clone().reader())?;
        let buckets_xml = root
            .get_mut_child("Buckets")
            .ok_or(ValidationErr::xml_error("<Buckets> tag not found"))?;

        let mut buckets: Vec<Bucket> = Vec::new();
        while let Some(b) = buckets_xml.take_child("Bucket") {
            let bucket = b;
            buckets.push(Bucket {
                name: get_text_result(&bucket, "Name")?,
                creation_date: from_iso8601utc(&get_text_result(&bucket, "CreationDate")?)?,
            })
        }
        Ok(buckets)
    }
}
