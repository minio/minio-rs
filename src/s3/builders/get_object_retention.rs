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
use crate::s3::response::GetObjectRetentionResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name};
use http::Method;

/// Argument builder for [get_object_retention()](Client::get_object_retention) API
#[derive(Clone, Debug, Default)]
pub struct GetObjectRetention {
    pub client: Option<Client>,

    pub extra_headers: Option<Multimap>,
    pub extra_query_params: Option<Multimap>,
    pub region: Option<String>,
    pub bucket: String,

    pub object: String,
    pub version_id: Option<String>,
}

impl GetObjectRetention {
    pub fn new(bucket: &str) -> Self {
        //TODO make bucket of type String because its cloned anyway
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
}

impl S3Api for GetObjectRetention {
    type S3Response = GetObjectRetentionResponse;
}

impl ToS3Request for GetObjectRetention {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

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
        query_params.insert(String::from("retention"), String::new());

        let client: &Client = self.client.as_ref().ok_or(Error::NoClientProvided)?;

        let req = S3Request::new(client, Method::GET)
            .region(self.region.as_deref())
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .headers(headers)
            .object(Some(&self.object));

        Ok(req)
    }
}
