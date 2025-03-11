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

use async_trait::async_trait;
use http::Method;

use crate::s3::response::ListBucketsResponse;
use crate::s3::{
    Client,
    error::Error,
    types::{S3Api, S3Request, ToS3Request},
    utils::Multimap,
};

/// Argument builder for [list_buckets()](Client::list_buckets) API.
#[derive(Clone, Debug, Default)]
pub struct ListBuckets {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
}

// builder interface
impl ListBuckets {
    pub fn new() -> Self {
        Default::default()
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
}

impl S3Api for ListBuckets {
    type S3Response = ListBucketsResponse;
}

#[async_trait]
impl ToS3Request for ListBuckets {
    async fn to_s3request(self) -> Result<S3Request, Error> {
        let headers: Multimap = self.extra_headers.unwrap_or_default();
        let query_params: Multimap = self.extra_query_params.unwrap_or_default();
        let client: Client = self.client.ok_or(Error::NoClientProvided)?;

        Ok(S3Request::new(client, Method::GET)
            .query_params(query_params)
            .headers(headers))
    }
}
