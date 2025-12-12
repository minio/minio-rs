// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! S3Request struct and implementation for executing HTTP requests.

use super::super::client::{DEFAULT_REGION, MinioClient};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, ObjectKey, Region};
use crate::s3::utils::ChecksumAlgorithm;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
/// Generic S3Request
pub struct S3Request {
    #[builder(!default)] // force required
    pub(crate) client: MinioClient,

    #[builder(!default)] // force required
    method: Method,

    #[builder(default)]
    region: Option<Region>,

    #[builder(default, setter(into))]
    pub(crate) bucket: Option<BucketName>,

    #[builder(default, setter(into))]
    pub(crate) object: Option<ObjectKey>,

    #[builder(default)]
    pub(crate) query_params: Multimap,

    #[builder(default)]
    headers: Multimap,

    #[builder(default, setter(into))]
    body: Option<Arc<SegmentedBytes>>,

    /// Optional trailing checksum algorithm for streaming uploads.
    ///
    /// When set, the request body will be sent using aws-chunked encoding
    /// with the checksum computed incrementally and appended as a trailer.
    #[builder(default)]
    pub(crate) trailing_checksum: Option<ChecksumAlgorithm>,

    /// When true and trailing checksums are enabled, signs each chunk with AWS Signature V4.
    ///
    /// Uses STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER where each chunk is signed
    /// and the trailer includes a trailer signature.
    #[builder(default = false)]
    pub(crate) use_signed_streaming: bool,

    /// region computed by [`S3Request::execute`]
    #[builder(default, setter(skip))]
    pub(crate) inner_region: Region,
}

impl S3Request {
    async fn compute_inner_region(&self) -> Result<Region, Error> {
        let region_str = match &self.bucket {
            Some(b) => {
                self.client
                    .get_region_cached(b.clone(), &self.region)
                    .await?
            }
            None => DEFAULT_REGION.as_str().to_string(),
        };
        Region::new(&region_str).map_err(Into::into)
    }

    /// Execute the request, returning the response. Only used in [`S3Api::send()`]
    pub async fn execute(&mut self) -> Result<reqwest::Response, Error> {
        self.inner_region = self.compute_inner_region().await?;

        self.client
            .execute(
                self.method.clone(),
                &self.inner_region,
                &mut self.headers,
                &self.query_params,
                &self.bucket.as_ref().map(|b| b.as_str()),
                &self.object.as_ref().map(|o| o.as_str()),
                self.body.as_ref().map(Arc::clone),
                self.trailing_checksum,
                self.use_signed_streaming,
            )
            .await
    }
}
