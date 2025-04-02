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
use crate::s3::response::SetObjectLockConfigResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{ObjectLockConfig, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name, insert};
use bytes::Bytes;
use http::Method;

/// Argument builder for [set_object_lock_config()](Client::set_object_lock_config) API

#[derive(Clone, Debug, Default)]
pub struct SetObjectLockConfig {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    config: ObjectLockConfig,
}

impl SetObjectLockConfig {
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

    pub fn config(mut self, config: ObjectLockConfig) -> Self {
        self.config = config;
        self
    }
}

impl S3Api for SetObjectLockConfig {
    type S3Response = SetObjectLockConfigResponse;
}

impl ToS3Request for SetObjectLockConfig {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;
        let client: Client = self.client.ok_or(Error::NoClientProvided)?;

        let bytes: Bytes = self.config.to_xml().into();
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));

        Ok(S3Request::new(client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "object-lock"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body))
    }
}
