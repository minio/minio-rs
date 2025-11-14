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

    #[builder(default, setter(into))]
    region: Option<String>,

    #[builder(default, setter(into))]
    pub(crate) bucket: Option<String>,

    #[builder(default, setter(into))]
    pub(crate) object: Option<String>,

    #[builder(default)]
    pub(crate) query_params: Multimap,

    #[builder(default)]
    headers: Multimap,

    #[builder(default, setter(into))]
    body: Option<Arc<SegmentedBytes>>,

    /// region computed by [`S3Request::execute`]
    #[builder(default, setter(skip))]
    pub(crate) inner_region: String,
}

impl S3Request {
    async fn compute_inner_region(&self) -> Result<String, Error> {
        Ok(match &self.bucket {
            Some(b) => self.client.get_region_cached(b, &self.region).await?,
            None => DEFAULT_REGION.to_string(),
        })
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
                &self.bucket.as_deref(),
                &self.object.as_deref(),
                self.body.as_ref().map(Arc::clone),
            )
            .await
    }
}
