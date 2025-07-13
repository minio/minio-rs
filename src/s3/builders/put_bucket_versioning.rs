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
use crate::s3::error::{MinioError, Result};
use crate::s3::multimap::Multimap;
use crate::s3::response::PutBucketVersioningResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use bytes::Bytes;
use http::Method;
use std::fmt;

/// Represents the versioning state of an S3 bucket.
///
/// This enum corresponds to the possible values returned by the
/// `GetBucketVersioning` API call in S3-compatible services.
///
/// # Variants
///
/// - `Enabled`: Object versioning is enabled for the bucket.
/// - `Suspended`: Object versioning is suspended for the bucket.
#[derive(Clone, Debug, PartialEq)]
pub enum VersioningStatus {
    /// Object versioning is enabled for the bucket.
    Enabled,
    /// Object versioning is suspended for the bucket.
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

/// Argument builder for the [`PutBucketVersioning`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::put_bucket_versioning`](crate::s3::client::Client::put_bucket_versioning) method.
#[derive(Clone, Debug, Default)]
pub struct PutBucketVersioning {
    /// The S3 client instance used to send the request.
    client: Client,

    /// Optional additional HTTP headers to include in the request.
    extra_headers: Option<Multimap>,

    /// Optional additional query parameters to include in the request URL.
    extra_query_params: Option<Multimap>,

    /// Optional AWS region to override the client's default region.
    region: Option<String>,

    /// The name of the bucket for which to configure versioning.
    bucket: String,

    /// Desired versioning status for the bucket.
    ///
    /// - `Some(VersioningStatus::Enabled)`: Enables versioning.
    /// - `Some(VersioningStatus::Suspended)`: Suspends versioning.
    /// - `None`: No change to the current versioning status.
    status: Option<VersioningStatus>,

    /// Specifies whether MFA delete is enabled for the bucket.
    ///
    /// - `Some(true)`: Enables MFA delete.
    /// - `Some(false)`: Disables MFA delete.
    /// - `None`: No change to the current MFA delete setting.
    mfa_delete: Option<bool>,
}

impl PutBucketVersioning {
    pub fn new(client: Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
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

    pub fn versioning_status(mut self, status: VersioningStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn mfa_delete(mut self, mfa_delete: Option<bool>) -> Self {
        self.mfa_delete = mfa_delete;
        self
    }
}

impl S3Api for PutBucketVersioning {
    type S3Response = PutBucketVersioningResponse;
}

impl ToS3Request for PutBucketVersioning {
    fn to_s3request(self) -> Result<S3Request> {
        check_bucket_name(&self.bucket, true)?;

        let data: String = {
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
                    return Err(MinioError::InvalidVersioningStatus(
                        "Missing VersioningStatus".into(),
                    ));
                }
            };

            data.push_str("</VersioningConfiguration>");
            data
        };
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(Bytes::from(data)));

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "versioning"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body))
    }
}
