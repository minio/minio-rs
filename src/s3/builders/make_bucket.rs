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
use crate::s3::client::DEFAULT_REGION;
use crate::s3::error::Error;
use crate::s3::http::BaseUrl;
use crate::s3::response::MakeBucketResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name};
use http::Method;

/// Argument builder for [make_bucket()](Client::make_bucket) API
#[derive(Clone, Debug, Default)]
pub struct MakeBucket {
    pub client: Option<Client>,

    pub extra_headers: Option<Multimap>,
    pub extra_query_params: Option<Multimap>,
    pub region: Option<String>,
    pub bucket: String,

    pub object_lock: bool,
}

impl MakeBucket {
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

    pub fn object_lock(mut self, object_lock: bool) -> Self {
        self.object_lock = object_lock;
        self
    }
}

#[derive(Default, Debug)]
pub struct MakeBucketPhantomData;

impl S3Api for MakeBucket {
    type S3Response = MakeBucketResponse;
}

impl ToS3Request for MakeBucket {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let base_url: &BaseUrl = match &self.client {
            None => return Err(Error::NoClientProvided),
            Some(c) => &c.base_url,
        };

        let region1: Option<&str> = self.region.as_deref();
        let region2: Option<&str> = if base_url.region.is_empty() {
            None
        } else {
            Some(base_url.region.as_str())
        };

        let region: &str = match (region1, region2) {
            (None, None) => DEFAULT_REGION,
            (Some(r), None) | (None, Some(r)) => r, // Take the non-None value
            (Some(r1), Some(r2)) if r1 == r2 => r1, // Both are Some and equal
            (Some(r1), Some(r2)) => {
                return Err(Error::RegionMismatch(r1.to_string(), r2.to_string()));
            }
        };

        let mut headers: Multimap = self
            .extra_headers
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();

        if self.object_lock {
            headers.insert(
                String::from("x-amz-bucket-object-lock-enabled"),
                String::from("true"),
            );
        }

        let query_params: Multimap = self
            .extra_query_params
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();

        let data: String = match region {
            DEFAULT_REGION => String::new(),
            _ => format!(
                "<CreateBucketConfiguration><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                region
            ),
        };

        let body: Option<SegmentedBytes> = match data.is_empty() {
            true => None,
            false => Some(SegmentedBytes::from(data)),
        };

        let client: &Client = self.client.as_ref().ok_or(Error::NoClientProvided)?;

        let req = S3Request::new(client, Method::PUT)
            .region(Some(region))
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .headers(headers)
            .body(body);

        //TODO insert into region_map used to be executed after creating the bucket...
        client
            .region_map
            .insert(self.bucket.clone(), region.to_string());

        Ok(req)
    }
}
