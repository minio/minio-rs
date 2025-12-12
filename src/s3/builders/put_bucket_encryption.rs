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

use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::Multimap;
use crate::s3::response::PutBucketEncryptionResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::SseConfig;
use crate::s3::types::{BucketName, Region, S3Api, S3Request, ToS3Request};
use crate::s3::utils::insert;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the [`PutBucketEncryption`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::put_bucket_encryption`](crate::s3::client::MinioClient::put_bucket_encryption) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutBucketEncryption {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(!default, setter(into))] // force required + accept Into<String>
    #[builder(!default)]
    bucket: BucketName,
    #[builder(default)]
    sse_config: SseConfig,
}

/// Builder type alias for [`PutBucketEncryption`].
///
/// Constructed via [`PutBucketEncryption::builder()`](PutBucketEncryption::builder) and used to build a [`PutBucketEncryption`] instance.
pub type PutBucketEncryptionBldr =
    PutBucketEncryptionBuilder<((MinioClient,), (), (), (), (BucketName,), ())>;

impl S3Api for PutBucketEncryption {
    type S3Response = PutBucketEncryptionResponse;
}

impl ToS3Request for PutBucketEncryption {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        let bytes: Bytes = self.sse_config.to_xml().into();
        let body = Arc::new(SegmentedBytes::from(bytes));

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(insert(self.extra_query_params, "encryption"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}
