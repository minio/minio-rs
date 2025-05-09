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
use crate::s3::types::{Bucket, FromS3Response, S3Request};
use crate::s3::utils::{from_iso8601utc, get_text};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of [list_buckets()](crate::s3::client::Client::list_buckets) API
#[derive(Debug, Clone)]
pub struct ListBucketsResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// the list of buckets that are present in the account.
    pub buckets: Vec<Bucket>,
}

#[async_trait]
impl FromS3Response for ListBucketsResponse {
    async fn from_s3response(
        _req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());

        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;
        let buckets_xml = root
            .get_mut_child("Buckets")
            .ok_or(Error::XmlError("<Buckets> tag not found".into()))?;

        let mut buckets: Vec<Bucket> = Vec::new();
        while let Some(b) = buckets_xml.take_child("Bucket") {
            let bucket = b;
            buckets.push(Bucket {
                name: get_text(&bucket, "Name")?,
                creation_date: from_iso8601utc(&get_text(&bucket, "CreationDate")?)?,
            })
        }

        Ok(Self { headers, buckets })
    }
}
