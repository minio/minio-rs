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

/// Response of
/// [get_bucket_tags()](crate::s3::client::Client::get_bucket_tags)
/// API
#[derive(Clone, Debug)]
pub struct GetBucketTagsResponse {
    /// Set of HTTP headers returned by the server.
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub tags: HashMap<String, String>,
}

#[async_trait]
impl FromS3Response for GetBucketTagsResponse {
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
