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

use std::marker::PhantomData;

use crate::s3::client::Client;
use crate::s3::multimap::Multimap;

/// Common parameters for bucket operations
#[derive(Clone, Debug, Default)]
pub struct BucketCommon<A> {
    pub(crate) client: Client,

    pub(crate) extra_headers: Option<Multimap>,
    pub(crate) extra_query_params: Option<Multimap>,
    pub(crate) region: Option<String>,
    pub(crate) bucket: String,

    _operation: PhantomData<A>,
}

impl<A: Default> BucketCommon<A> {
    pub fn new(client: Client, bucket: String) -> BucketCommon<A> {
        BucketCommon {
            client,
            bucket,
            ..Default::default()
        }
    }

    /// Sets extra headers for the request
    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    /// Sets extra query parameters for the request
    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}
