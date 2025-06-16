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
use crate::s3::error::Error;
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::SelectObjectContentResponse;
use crate::s3::sse::SseCustomerKey;
use crate::s3::types::{S3Api, S3Request, SelectRequest, ToS3Request};
use crate::s3::utils::{check_bucket_name, check_object_name, insert, md5sum_hash};
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;

/// Argument builder for the [`SelectObjectContent`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::select_object_content`](crate::s3::client::Client::select_object_content) method.
#[derive(Default)]
pub struct SelectObjectContent {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    version_id: Option<String>,
    ssec: Option<SseCustomerKey>,
    request: SelectRequest,
}

impl SelectObjectContent {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
            client,
            bucket,
            object,
            ..Default::default()
        }
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }

    pub fn ssec(mut self, ssec: Option<SseCustomerKey>) -> Self {
        self.ssec = ssec;
        self
    }

    pub fn request(mut self, request: SelectRequest) -> Self {
        self.request = request;
        self
    }
}

impl S3Api for SelectObjectContent {
    type S3Response = SelectObjectContentResponse;
}

#[async_trait]
impl ToS3Request for SelectObjectContent {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;

            if self.ssec.is_some() && !self.client.is_secure() {
                return Err(Error::SseTlsRequired(None));
            }
        }
        let bytes: Bytes = self.request.to_xml().into();

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        headers.add("Content-MD5", md5sum_hash(bytes.as_ref()));

        let mut query_params: Multimap = insert(self.extra_query_params, "select");
        query_params.add("select-type", "2");

        Ok(S3Request::new(self.client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(headers)
            .object(Some(self.object))
            .body(Some(bytes.into())))
    }
}
