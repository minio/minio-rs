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
use crate::s3::response::SetBucketPolicyResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name};
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;

/// Argument builder for [set_bucket_policy()](crate::s3::client::Client::set_bucket_policy) API
#[derive(Clone, Debug, Default)]
pub struct SetBucketPolicy {
    pub(crate) client: Option<Client>,

    pub(crate) extra_headers: Option<Multimap>,
    pub(crate) extra_query_params: Option<Multimap>,
    pub(crate) region: Option<String>,
    pub(crate) bucket: String,

    pub(crate) config: String, //TODO consider PolicyConfig struct
}

impl SetBucketPolicy {
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

    pub fn config(mut self, config: String) -> Self {
        self.config = config;
        self
    }
}

impl S3Api for SetBucketPolicy {
    type S3Response = SetBucketPolicyResponse;
}

#[async_trait]
impl ToS3Request for SetBucketPolicy {
    async fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let headers: Multimap = self.extra_headers.unwrap_or_default();
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.insert(String::from("policy"), String::new());

        let bytes: Bytes = self.config.to_string().into();
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));

        let client: Client = self.client.ok_or(Error::NoClientProvided)?;

        Ok(S3Request::new(client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(headers)
            .body(body))
    }
}
