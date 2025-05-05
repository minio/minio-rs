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
use crate::s3::response::SetObjectRetentionResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{RetentionMode, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{
    UtcTime, check_bucket_name, check_object_name, insert, md5sum_hash, to_iso8601utc,
};
use bytes::Bytes;
use http::Method;

/// Argument builder for [set_object_retention()](crate::s3::client::Client::set_object_retention) API
#[derive(Clone, Debug, Default)]
pub struct SetObjectRetention {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    version_id: Option<String>,
    bypass_governance_mode: bool,
    retention_mode: Option<RetentionMode>,
    retain_until_date: Option<UtcTime>,
}

impl SetObjectRetention {
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
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

impl ToS3Request for SetObjectRetention {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;

            if self.retention_mode.is_some() ^ self.retain_until_date.is_some() {
                return Err(Error::InvalidRetentionConfig(String::from(
                    "both mode and retain_until_date must be set or unset",
                )));
            }
        }

        let data: String = {
            let mut data: String = "<Retention>".into();
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
            data
        };

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        if self.bypass_governance_mode {
            headers.add("x-amz-bypass-governance-retention", "true");
        }
        headers.add("Content-MD5", md5sum_hash(data.as_ref()));

        let mut query_params: Multimap = insert(self.extra_query_params, "retention");
        query_params.add_version(self.version_id);

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(headers)
            .object(Some(self.object))
            .body(Some(SegmentedBytes::from(Bytes::from(data)))))
    }
}
