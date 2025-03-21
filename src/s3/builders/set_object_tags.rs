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
use crate::s3::builders::SegmentedBytes;
use crate::s3::error::Error;
use crate::s3::response::SetObjectTagsResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name};
use bytes::Bytes;
use http::Method;
use std::collections::HashMap;

/// Argument builder for [set_object_tags()](Client::set_object_tags) API
#[derive(Clone, Debug, Default)]
pub struct SetObjectTags {
    pub client: Option<Client>,

    pub extra_headers: Option<Multimap>,
    pub extra_query_params: Option<Multimap>,
    pub region: Option<String>,
    pub bucket: String,

    pub object: String,
    pub version_id: Option<String>,
    pub tags: HashMap<String, String>,
}

impl SetObjectTags {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            ..Default::default()
        }
    }
    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    pub fn object(mut self, object: String) -> Self {
        self.object = object;
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
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        // TODO add to all other function (that use object) the following test
        // TODO should it be moved to the object setter function? or use validate as in put_object
        if self.object.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        let headers = self
            .extra_headers
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();
        let mut query_params = self
            .extra_query_params
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();

        if let Some(v) = &self.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }
        query_params.insert("tagging".into(), String::new());

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

        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(Bytes::from(data)));
        let client: &Client = self.client.as_ref().ok_or(Error::NoClientProvided)?;

        let req = S3Request::new(client, Method::PUT)
            .region(self.region.as_deref())
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .object(Some(&self.object))
            .headers(headers)
            .body(body);

        Ok(req)
    }
}
