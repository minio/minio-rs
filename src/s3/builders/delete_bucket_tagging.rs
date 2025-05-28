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
use crate::s3::response::DeleteBucketTaggingResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use http::Method;

/// Argument builder for the [`DeleteBucketTagging`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::delete_bucket_tags`](crate::s3::client::Client::delete_bucket_tagging) method.
pub type DeleteBucketTagging = BucketCommon<DeleteBucketTaggingPhantomData>;

#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct DeleteBucketTaggingPhantomData;

impl S3Api for DeleteBucketTagging {
    type S3Response = DeleteBucketTaggingResponse;
}

impl ToS3Request for DeleteBucketTagging {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        Ok(S3Request::new(self.client, Method::DELETE)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "tagging"))
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
