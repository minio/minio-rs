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

use crate::s3::error::{Error, ValidationErr};
use crate::s3::lifecycle_config::LifecycleConfig;
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use chrono::{DateTime, NaiveDateTime, Utc};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_lifecycle`](crate::s3::client::MinioClient::get_bucket_lifecycle) API call,
/// providing the lifecycle configuration of an S3 bucket.
///
/// The lifecycle configuration defines rules for managing the lifecycle of objects in the bucket,
/// such as transitioning objects to different storage classes or expiring them after a specified period.
///
/// For more information, refer to the [AWS S3 GetBucketLifecycleConfiguration API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html).
#[derive(Clone, Debug)]
pub struct GetBucketLifecycleResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketLifecycleResponse);
impl_has_s3fields!(GetBucketLifecycleResponse);

impl HasBucket for GetBucketLifecycleResponse {}
impl HasRegion for GetBucketLifecycleResponse {}

impl GetBucketLifecycleResponse {
    /// Returns the lifecycle configuration of the bucket.
    ///
    /// This configuration includes rules for managing the lifecycle of objects in the bucket,
    /// such as transitioning them to different storage classes or expiring them after a specified period.
    pub fn config(&self) -> Result<LifecycleConfig, ValidationErr> {
        LifecycleConfig::from_xml(&Element::parse(self.body.clone().reader())?)
    }

    /// Returns the last update time of the lifecycle configuration
    /// (`X-Minio-LifecycleConfig-UpdatedAt`), if available.
    pub fn updated_at(&self) -> Option<DateTime<Utc>> {
        self.headers
            .get("x-minio-lifecycleconfig-updatedat")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                NaiveDateTime::parse_from_str(v, "%Y%m%dT%H%M%SZ")
                    .ok()
                    .map(|naive| DateTime::from_naive_utc_and_offset(naive, Utc))
            })
    }
}
