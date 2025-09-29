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

use crate::s3::MinioClient;
use crate::s3::builders::{BucketCommon, BucketCommonBuilder};
use crate::s3::error::ValidationErr;
use crate::s3::response::GetObjectLockConfigResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use http::Method;

/// Argument builder for the [`GetObjectLockConfig`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::get_object_lock_config`](crate::s3::client::MinioClient::get_object_lock_config) method.
pub type GetObjectLockConfig = BucketCommon<GetObjectLockConfigPhantomData>;

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct GetObjectLockConfigPhantomData;

pub type GetObjectLockConfigBldr = BucketCommonBuilder<
    GetObjectLockConfigPhantomData,
    ((MinioClient,), (), (), (), (String,), ()),
>;

impl S3Api for GetObjectLockConfig {
    type S3Response = GetObjectLockConfigResponse;
}

impl ToS3Request for GetObjectLockConfig {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(insert(self.extra_query_params, "object-lock"))
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}
