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

use crate::s3::builders::SegmentedBytes;
use crate::s3::error::Error;
use crate::s3::response::SetBucketVersioningResponse;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, Multimap};
use crate::s3::Client;
use bytes::Bytes;
use http::Method;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum VersioningStatus {
    /// **Enable** object versioning in given bucket.
    Enabled,
    /// **Suspend** object versioning in given bucket.
    Suspended,
}

impl fmt::Display for VersioningStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersioningStatus::Enabled => write!(f, "Enabled"),
            VersioningStatus::Suspended => write!(f, "Suspended"),
        }
    }
}

/// Argument builder for [set_bucket_encryption()](Client::set_bucket_encryption) API
#[derive(Clone, Debug, Default)]
pub struct SetBucketVersioning {
    pub(crate) client: Option<Client>,

    pub(crate) extra_headers: Option<Multimap>,
    pub(crate) extra_query_params: Option<Multimap>,
    pub(crate) region: Option<String>,
    pub(crate) bucket: String,

    pub(crate) status: Option<VersioningStatus>,
    pub(crate) mfa_delete: Option<bool>,
}

impl SetBucketVersioning {
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

    pub fn versioning_status(mut self, status: VersioningStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn mfa_delete(mut self, mfa_delete: Option<bool>) -> Self {
        self.mfa_delete = mfa_delete;
        self
    }
}

impl S3Api for SetBucketVersioning {
    type S3Response = SetBucketVersioningResponse;
}

impl ToS3Request for SetBucketVersioning {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let headers = self
            .extra_headers
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();
        let mut query_params = self
            .extra_query_params
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .unwrap_or_default();

        query_params.insert("versioning".into(), String::new());

        let mut data = "<VersioningConfiguration>".to_string();

        if let Some(v) = self.mfa_delete {
            data.push_str("<MFADelete>");
            data.push_str(if v { "Enabled" } else { "Disabled" });
            data.push_str("</MFADelete>");
        }

        match self.status {
            Some(VersioningStatus::Enabled) => data.push_str("<Status>Enabled</Status>"),
            Some(VersioningStatus::Suspended) => data.push_str("<Status>Suspended</Status>"),
            None => {
                return Err(Error::InvalidVersioningStatus(
                    "Missing VersioningStatus".into(),
                ))
            }
        };

        data.push_str("</VersioningConfiguration>");

        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(Bytes::from(data)));
        let client: &Client = self.client.as_ref().ok_or(Error::NoClientProvided)?;

        let req = S3Request::new(client, Method::PUT)
            .region(self.region.as_deref())
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .headers(headers)
            .body(body);

        Ok(req)
    }
}
