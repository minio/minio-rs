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
use crate::s3::error::Error;
use crate::s3::multimap::Multimap;
use crate::s3::response::GetRegionResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use http::Method;

/// This struct constructs the parameters required for the [`Client::get_region`](crate::s3::client::Client::get_region) method.
#[derive(Clone, Debug, Default)]
pub struct GetRegion {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    bucket: String,
}

impl GetRegion {
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
}

#[derive(Default, Debug)]
pub struct GetRegionPhantomData;

impl S3Api for GetRegion {
    type S3Response = GetRegionResponse;
}

impl ToS3Request for GetRegion {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        Ok(S3Request::new(self.client, Method::GET)
            .region(Some(DEFAULT_REGION.to_string()))
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "location"))
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
