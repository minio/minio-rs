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
use crate::s3::lifecycle_config::LifecycleConfig;
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::PutBucketLifecycleResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert, md5sum_hash};
use bytes::Bytes;
use http::Method;

/// Argument builder for the [`PutBucketLifecycle`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycle.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::put_bucket_lifecycle`](crate::s3::client::Client::put_bucket_lifecycle) method.
#[derive(Clone, Debug, Default)]
pub struct PutBucketLifecycle {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    config: LifecycleConfig,
}

impl PutBucketLifecycle {
    pub fn new(client: Client, bucket: String) -> Self {
        Self {
            client,
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

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn life_cycle_config(mut self, config: LifecycleConfig) -> Self {
        self.config = config;
        self
    }
}

impl S3Api for PutBucketLifecycle {
    type S3Response = PutBucketLifecycleResponse;
}

impl ToS3Request for PutBucketLifecycle {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();

        let bytes: Bytes = self.config.to_xml().into();
        headers.add("Content-MD5", md5sum_hash(&bytes));
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "lifecycle"))
            .headers(headers)
            .body(body))
    }
}
