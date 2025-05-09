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
use crate::s3::utils::{get_text, take_bucket};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::collections::HashMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_tagging`](crate::s3::client::Client::get_bucket_tagging) API call,
/// providing the set of tags associated with an S3 bucket.
///
/// Tags are key-value pairs that help organize and manage resources,
/// often used for cost allocation and access control.
///
/// For more information, refer to the [AWS S3 GetBucketTagging API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html).
#[derive(Clone, Debug)]
pub struct GetBucketTaggingResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose tags are retrieved.
    pub bucket: String,

    /// A collection of tags assigned to the bucket.
    ///
    /// Each tag is a key-value pair represented as a `HashMap<String, String>`.
    /// If the bucket has no tags, this map will be empty.
    ///
    /// Note: If the bucket has no tags, the `get_bucket_tags` API call may return an error
    /// with the code `NoSuchTagSet`. It's advisable to handle this case appropriately in your application.
    pub tags: HashMap<String, String>,
}

#[async_trait]
impl FromS3Response for GetBucketTaggingResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => {
                let headers: HeaderMap = mem::take(r.headers_mut());
                let body = r.bytes().await?;
                let mut root = Element::parse(body.reader())?;

                let element = root
                    .get_mut_child("TagSet")
                    .ok_or(Error::XmlError("<TagSet> tag not found".to_string()))?;
                let mut tags = HashMap::new();
                while let Some(v) = element.take_child("Tag") {
                    tags.insert(get_text(&v, "Key")?, get_text(&v, "Value")?);
                }

                Ok(Self {
                    headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    tags,
                })
            }
            Err(Error::S3Error(e)) if e.code == ErrorCode::NoSuchTagSet => Ok(Self {
                headers: e.headers,
                region: req.inner_region,
                bucket: take_bucket(req.bucket)?,
                tags: HashMap::new(),
            }),
            Err(e) => Err(e),
        }
    }
}
