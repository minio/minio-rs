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

use http::Method;
use std::sync::Arc;

use crate::s3::multimap::Multimap;
use crate::s3::response::ListBucketsResponse;
use crate::s3::{
    Client,
    error::Error,
    types::{S3Api, S3Request, ToS3Request},
};

/// Argument builder for [list_buckets()](Client::list_buckets) API.
#[derive(Clone, Debug, Default)]
pub struct ListBuckets {
    client: Arc<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
}

impl ListBuckets {
    pub fn new(client: &Arc<Client>) -> Self {
        Self {
            client: Arc::clone(client),
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
    fn to_s3request(self) -> Result<S3Request, Error> {
        Ok(S3Request::new(self.client, Method::GET)
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
