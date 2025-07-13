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
use crate::s3::client::DEFAULT_REGION;
use crate::s3::error::{MinioError, Result};
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::CreateBucketResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use http::Method;

/// Argument builder for the [`CreateBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::create_bucket`](crate::s3::client::Client::create_bucket) method.
#[derive(Clone, Debug, Default)]
pub struct CreateBucket {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object_lock: bool,
}

impl CreateBucket {
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

    pub fn object_lock(mut self, object_lock: bool) -> Self {
        self.object_lock = object_lock;
        self
    }
}

impl S3Api for CreateBucket {
    type S3Response = CreateBucketResponse;
}

impl ToS3Request for CreateBucket {
    fn to_s3request(self) -> Result<S3Request> {
        check_bucket_name(&self.bucket, true)?;

        let region1: Option<&str> = self.region.as_deref();
        let region2: Option<&str> = self.client.get_region_from_url();

        let region_str: String = match (region1, region2) {
            (None, None) => DEFAULT_REGION.to_string(),
            (Some(_), None) => self.region.unwrap(),
            (None, Some(v)) => v.to_string(),
            (Some(r1), Some(r2)) if r1 == r2 => self.region.unwrap(), // Both are Some and equal
            (Some(r1), Some(r2)) => {
                return Err(MinioError::RegionMismatch {
                    bucket_region: r1.to_string(),
                    region: r2.to_string(),
                });
            }
        };

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        if self.object_lock {
            headers.add("x-amz-bucket-object-lock-enabled", "true");
        }

        let data: String = match region_str.as_str() {
            DEFAULT_REGION => String::new(),
            _ => format!(
                "<CreateBucketConfiguration><LocationConstraint>{region_str}</LocationConstraint></CreateBucketConfiguration>",
            ),
        };

        let body: Option<SegmentedBytes> = match data.is_empty() {
            true => None,
            false => Some(SegmentedBytes::from(data)),
        };

        Ok(S3Request::new(self.client, Method::PUT)
            .region(Some(region_str))
            .bucket(Some(self.bucket))
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(headers)
            .body(body))
    }
}
