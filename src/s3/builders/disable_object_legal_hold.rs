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
use crate::s3::response::DisableObjectLegalHoldResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, check_object_name, insert, md5sum_hash};
use bytes::Bytes;
use http::Method;

/// Argument builder for [disable_object_legal_hold()](Client::disable_object_legal_hold) API
#[derive(Clone, Debug, Default)]
pub struct DisableObjectLegalHold {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    version_id: Option<String>,
}

impl DisableObjectLegalHold {
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

    pub fn version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }
}

impl S3Api for DisableObjectLegalHold {
    type S3Response = DisableObjectLegalHoldResponse;
}

impl ToS3Request for DisableObjectLegalHold {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        let mut query_params: Multimap = insert(self.extra_query_params, "legal-hold");
        query_params.add_version(self.version_id);

        const PAYLOAD: &str = "<LegalHold><Status>OFF</Status></LegalHold>";
        headers.add("Content-MD5", md5sum_hash(PAYLOAD.as_ref()));
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(Bytes::from(PAYLOAD)));
        //TODO consider const body

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(headers)
            .object(Some(self.object))
            .body(body))
    }
}
