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
use crate::s3::response::SelectObjectContentResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::sse::SseCustomerKey;
use crate::s3::types::{S3Api, S3Request, SelectRequest, ToS3Request};
use crate::s3::utils::{Multimap, check_bucket_name, check_object_name, insert, md5sum_hash};
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;

/// Argument builder for [bucket_exists()](Client::bucket_exists) API
#[derive(Default)]
pub struct SelectObjectContent {
    client: Option<Client>,

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
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn object(mut self, object: String) -> Self {
        self.object = object;
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
        let client: Client = self.client.ok_or(Error::NoClientProvided)?;
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;

            if self.ssec.is_some() && !client.base_url.https {
                return Err(Error::SseTlsRequired(None));
            }
        }
        let region: String = client.get_region_cached(&self.bucket, &self.region)?;
        let data = self.request.to_xml();
        let bytes: Bytes = data.into();

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        headers.insert("Content-MD5".into(), md5sum_hash(bytes.as_ref()));

        let mut query_params: Multimap = insert(self.extra_query_params, "select");
        query_params.insert("select-type".into(), "2".into());

        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));

        Ok(S3Request::new(client, Method::POST)
            .region(Some(region))
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .object(Some(self.object))
            .headers(headers)
            .body(body))
    }
}
