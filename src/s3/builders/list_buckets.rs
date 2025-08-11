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
use crate::s3::multimap::Multimap;
use crate::s3::response::ListBucketsResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use http::Method;

/// Argument builder for the [`ListBuckets`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::list_buckets`](crate::s3::client::Client::list_buckets) method.
#[derive(Clone, Debug, Default)]
pub struct ListBuckets {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
}

impl ListBuckets {
    pub fn new(client: Client) -> Self {
        Self {
            client,
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

impl S3Api for ListBuckets {
    type S3Response = ListBucketsResponse;
}

impl ToS3Request for ListBuckets {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        Ok(S3Request::new(self.client, Method::GET)
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
