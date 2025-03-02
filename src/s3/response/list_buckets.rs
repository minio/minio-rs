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
use xmltree::Element;

/// Response of [list_buckets()](crate::s3::client::Client::list_buckets) API
#[derive(Debug, Clone)]
pub struct ListBucketsResponse {
    pub headers: HeaderMap,
    pub buckets: Vec<Bucket>,
}

#[async_trait]
impl FromS3Response for ListBucketsResponse {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = resp?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;
        let buckets = root
            .get_mut_child("Buckets")
            .ok_or(Error::XmlError(String::from("<Buckets> tag not found")))?;

        let mut bucket_list: Vec<Bucket> = Vec::new();
        while let Some(b) = buckets.take_child("Bucket") {
            let bucket = b;
            bucket_list.push(Bucket {
                name: get_text(&bucket, "Name")?,
                creation_date: from_iso8601utc(&get_text(&bucket, "CreationDate")?)?,
            })
        }

        Ok(ListBucketsResponse {
            headers: header_map.clone(),
            buckets: bucket_list,
        })
    }
}
