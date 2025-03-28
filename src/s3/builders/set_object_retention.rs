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
use crate::s3::builders::SegmentedBytes;
use crate::s3::error::Error;
use crate::s3::response::SetObjectRetentionResponse;
use crate::s3::types::{RetentionMode, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{Multimap, UtcTime, check_bucket_name, md5sum_hash, to_iso8601utc};
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;

/// Argument builder for [set_object_retention()](Client::set_object_retention) API
#[derive(Clone, Debug, Default)]
pub struct SetObjectRetention {
    pub client: Option<Client>,

    pub extra_headers: Option<Multimap>,
    pub extra_query_params: Option<Multimap>,
    pub region: Option<String>,
    pub bucket: String,

    pub object: String,
    pub version_id: Option<String>,
    pub bypass_governance_mode: bool,
    pub retention_mode: Option<RetentionMode>,
    pub retain_until_date: Option<UtcTime>,
}

impl SetObjectRetention {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            bypass_governance_mode: false,
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

    pub fn bypass_governance_mode(mut self, bypass_governance_mode: bool) -> Self {
        self.bypass_governance_mode = bypass_governance_mode;
        self
    }

    pub fn retention_mode(mut self, retention_mode: Option<RetentionMode>) -> Self {
        self.retention_mode = retention_mode;
        self
    }

    pub fn retain_until_date(mut self, retain_until_date: Option<UtcTime>) -> Self {
        self.retain_until_date = retain_until_date;
        self
    }
}

impl S3Api for SetObjectRetention {
    type S3Response = SetObjectRetentionResponse;
}

#[async_trait]
impl ToS3Request for SetObjectRetention {
    async fn to_s3request(self) -> Result<S3Request, Error> {
        //TODO move the following checks to a validate fn
        check_bucket_name(&self.bucket, true)?;

        if self.object.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if self.retention_mode.is_some() ^ self.retain_until_date.is_some() {
            return Err(Error::InvalidRetentionConfig(String::from(
                "both mode and retain_until_date must be set or unset",
            )));
        }

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();

        if self.bypass_governance_mode {
            headers.insert(
                String::from("x-amz-bypass-governance-retention"),
                String::from("true"),
            );
        }

        if let Some(v) = &self.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }
        query_params.insert(String::from("retention"), String::new());

        let mut data: String = String::from("<Retention>");
        if let Some(v) = &self.retention_mode {
            data.push_str("<Mode>");
            data.push_str(&v.to_string());
            data.push_str("</Mode>");
        }
        if let Some(v) = &self.retain_until_date {
            data.push_str("<RetainUntilDate>");
            data.push_str(&to_iso8601utc(*v));
            data.push_str("</RetainUntilDate>");
        }
        data.push_str("</Retention>");

        headers.insert(String::from("Content-MD5"), md5sum_hash(data.as_ref()));

        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(Bytes::from(data)));
        let client: Client = self.client.ok_or(Error::NoClientProvided)?;

        Ok(S3Request::new(client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(headers)
            .object(Some(self.object))
            .body(body))
    }
}
