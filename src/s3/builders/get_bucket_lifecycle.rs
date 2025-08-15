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
use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::GetBucketLifecycleResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use http::Method;

/// Argument builder for the [`GetBucketLifecycle`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycle.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::get_bucket_lifecycle`](crate::s3::client::Client::get_bucket_lifecycle) method.
#[derive(Clone, Debug, Default)]
pub struct GetBucketLifecycle {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    with_updated_at: bool,
}

impl GetBucketLifecycle {
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

    pub fn with_updated_at(mut self, with_updated_at: bool) -> Self {
        self.with_updated_at = with_updated_at;
        self
    }
}

impl S3Api for GetBucketLifecycle {
    type S3Response = GetBucketLifecycleResponse;
}

impl ToS3Request for GetBucketLifecycle {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut query_params: Multimap = insert(self.extra_query_params, "lifecycle");
        if self.with_updated_at {
            query_params.add("withUpdatedAt", "true");
        }

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
