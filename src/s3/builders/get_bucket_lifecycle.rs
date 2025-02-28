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

use crate::s3::builders::BucketCommon;
use crate::s3::error::Error;
use crate::s3::response::GetBucketLifecycleResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use crate::s3::Client;
use http::Method;

/// Argument builder for [get_bucket_lifecycle()](Client::get_bucket_lifecycle) API
pub type GetBucketLifecycle = BucketCommon<GetBucketLifecyclePhantomData>;

#[derive(Default, Debug)]
pub struct GetBucketLifecyclePhantomData;

impl S3Api for GetBucketLifecycle {
    type S3Response = GetBucketLifecycleResponse;
}

impl ToS3Request for GetBucketLifecycle {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let headers = self
            .extra_headers
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();
        let mut query_params = self
            .extra_query_params
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();

        query_params.insert("lifecycle".into(), String::new());

        let client: &Client = self.client.as_ref().ok_or(Error::NoClientProvided)?;

        let req = S3Request::new(client, Method::GET)
            .region(self.region.as_deref())
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .headers(headers);

        Ok(req)
    }
}
