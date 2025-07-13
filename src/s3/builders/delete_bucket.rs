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
use crate::s3::error::Result;
use crate::s3::response::DeleteBucketResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use http::Method;

/// Argument builder for the [`DeleteBucket`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::delete_bucket`](crate::s3::client::Client::delete_bucket) method.
/// See [Amazon S3: Deleting Buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/delete-bucket.html) for more information.
pub type DeleteBucket = BucketCommon<DeleteBucketPhantomData>;

#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct DeleteBucketPhantomData;

impl S3Api for DeleteBucket {
    type S3Response = DeleteBucketResponse;
}

impl ToS3Request for DeleteBucket {
    fn to_s3request(self) -> Result<S3Request> {
        check_bucket_name(&self.bucket, true)?;

        Ok(S3Request::new(self.client, Method::DELETE)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
