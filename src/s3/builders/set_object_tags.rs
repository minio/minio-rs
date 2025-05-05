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
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::SetObjectTagsResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, check_object_name, insert};
use bytes::Bytes;
use http::Method;
use std::collections::HashMap;

/// Argument builder for [set_object_tags()](crate::s3::client::Client::set_object_tags) API
#[derive(Clone, Debug, Default)]
pub struct SetObjectTags {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    version_id: Option<String>,
    tags: HashMap<String, String>,
}

impl SetObjectTags {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
            client,
            bucket,
            object,
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

    pub fn version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }

    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }
}

impl S3Api for SetObjectTags {
    type S3Response = SetObjectTagsResponse;
}

impl ToS3Request for SetObjectTags {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let mut query_params: Multimap = insert(self.extra_query_params, "tagging");
        query_params.add_version(self.version_id);

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
            .query_params(query_params)
            .object(Some(self.object))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body))
    }
}
