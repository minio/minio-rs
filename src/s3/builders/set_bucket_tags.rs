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

use crate::s3::Client;
use crate::s3::error::Error;
use crate::s3::response::SetBucketTagsResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name, insert};
use bytes::Bytes;
use http::Method;
use std::collections::HashMap;
use std::sync::Arc;

/// Argument builder for [set_bucket_tags()](crate::s3::client::Client::set_bucket_tags) API
#[derive(Clone, Debug, Default)]
pub struct SetBucketTags {
    client: Arc<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    tags: HashMap<String, String>,
}

impl SetBucketTags {
    pub fn new(client: &Arc<Client>, bucket: String) -> Self {
        Self {
            client: Arc::clone(client),
            bucket,
            ..Default::default()
        }
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }
}

impl S3Api for SetBucketTags {
    type S3Response = SetBucketTagsResponse;
}

impl ToS3Request for SetBucketTags {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let data: String = {
            let mut data = String::from("<Tagging>");
            if !self.tags.is_empty() {
                data.push_str("<TagSet>");
                for (key, value) in self.tags.iter() {
                    data.push_str("<Tag>");
                    data.push_str("<Key>");
                    data.push_str(key);
                    data.push_str("</Key>");
                    data.push_str("<Value>");
                    data.push_str(value);
                    data.push_str("</Value>");
                    data.push_str("</Tag>");
                }
                data.push_str("</TagSet>");
            }
            data.push_str("</Tagging>");
            data
        };
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(Bytes::from(data)));

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "tagging"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body))
    }
}
