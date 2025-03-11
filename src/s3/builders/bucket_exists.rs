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
use crate::s3::builders::BucketCommon;
use crate::s3::error::Error;
use crate::s3::response::BucketExistsResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name};
use async_trait::async_trait;
use http::Method;

/// Argument builder for [bucket_exists()](Client::bucket_exists) API
pub type BucketExists = BucketCommon<BucketExistsPhantomData>;

#[derive(Default, Debug)]
pub struct BucketExistsPhantomData;

impl S3Api for BucketExists {
    type S3Response = BucketExistsResponse;
}

#[async_trait]
impl ToS3Request for BucketExists {
    async fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let headers: Multimap = self.extra_headers.unwrap_or_default();
        let query_params: Multimap = self.extra_query_params.unwrap_or_default();
        let client: Client = self.client.ok_or(Error::NoClientProvided)?;

        Ok(S3Request::new(client, Method::HEAD)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(headers))
    }
}
