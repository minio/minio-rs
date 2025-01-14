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

use crate::s3::{
    client::Client,
    error::Error,
    utils::{check_bucket_name, Multimap},
};

#[derive(Clone, Debug, Default)]
pub struct BucketCommon<A> {
    pub(crate) client: Option<Client>,

    pub(crate) extra_headers: Option<Multimap>,
    pub(crate) extra_query_params: Option<Multimap>,
    pub(crate) region: Option<String>,
    pub(crate) bucket: String,

    _operation: PhantomData<A>,
}

impl<A: Default> BucketCommon<A> {
    pub fn new(bucket: &str) -> Result<BucketCommon<A>, Error> {
        check_bucket_name(bucket, true)?;
        Ok(BucketCommon {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
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
}
