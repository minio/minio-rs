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

use crate::s3::client::DEFAULT_REGION;
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::Multimap;
use crate::s3::response::GetRegionResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [`GetRegion`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::get_region`](crate::s3::client::MinioClient::get_region) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetRegion {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(setter(into))] // force required + accept Into<String>
    bucket: String,
}

/// Builder type alias for [`GetRegion`].
///
/// Constructed via [`GetRegion::builder()`](GetRegion::builder) and used to build a [`GetRegion`] instance.
pub type GetRegionBldr = GetRegionBuilder<((MinioClient,), (), (), (String,))>;

#[doc(hidden)]
#[derive(Default, Debug)]
pub struct GetRegionPhantomData;

impl S3Api for GetRegion {
    type S3Response = GetRegionResponse;
}

impl ToS3Request for GetRegion {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(DEFAULT_REGION.to_string())
            .bucket(self.bucket)
            .query_params(insert(self.extra_query_params, "location"))
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}
