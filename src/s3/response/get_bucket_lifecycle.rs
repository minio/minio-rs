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

use crate::s3::error::Error;
use crate::s3::lifecycle_config::LifecycleConfig;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{UtcTime, take_bucket};
use async_trait::async_trait;
use bytes::Buf;
use chrono::{DateTime, NaiveDateTime, Utc};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_lifecycle`](crate::s3::client::Client::get_bucket_lifecycle) API call,
/// providing the lifecycle configuration of an S3 bucket.
///
/// The lifecycle configuration defines rules for managing the lifecycle of objects in the bucket,
/// such as transitioning objects to different storage classes or expiring them after a specified period.
///
/// For more information, refer to the [AWS S3 GetBucketLifecycleConfiguration API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html).
#[derive(Clone, Debug)]
pub struct GetBucketLifecycleResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose lifecycle configuration is retrieved.
    pub bucket: String,

    /// The lifecycle configuration of the bucket.
    ///
    /// This includes a set of rules that define actions applied to objects, such as transitioning
    /// them to different storage classes, expiring them, or aborting incomplete multipart uploads.
    ///
    /// If the bucket has no lifecycle configuration, this field may contain an empty configuration.
    pub config: LifecycleConfig,

    /// Optional value of `X-Minio-LifecycleConfig-UpdatedAt` header, indicating the last update
    /// time of the lifecycle configuration.
    pub updated_at: Option<UtcTime>,
}

#[async_trait]
impl FromS3Response for GetBucketLifecycleResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());
        let config: LifecycleConfig = {
            let body = resp.bytes().await?;
            let mut root = Element::parse(body.reader())?;
            LifecycleConfig::from_xml(&mut root)?
        };
        let updated_at: Option<DateTime<Utc>> = headers
            .get("x-minio-lifecycleconfig-updatedat")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                NaiveDateTime::parse_from_str(v, "%Y%m%dT%H%M%SZ")
                    .ok()
                    .map(|naive| DateTime::from_naive_utc_and_offset(naive, Utc))
            });

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            config,
            updated_at,
        })
    }
}
