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

use crate::s3::builders::BucketCommon;
use crate::s3::error::Error;
use crate::s3::response::DeleteObjectLockConfigResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{ObjectLockConfig, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use bytes::Bytes;
use http::Method;

/// This struct constructs the parameters required for the [`Client::delete_object_lock_config`](crate::s3::client::Client::delete_object_lock_config) method.
pub type DeleteObjectLockConfig = BucketCommon<DeleteObjectLockConfigPhantomData>;

#[derive(Clone, Debug, Default)]
pub struct DeleteObjectLockConfigPhantomData;

impl S3Api for DeleteObjectLockConfig {
    type S3Response = DeleteObjectLockConfigResponse;
}

impl ToS3Request for DeleteObjectLockConfig {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let config = ObjectLockConfig {
            retention_mode: None,
            retention_duration_days: None,
            retention_duration_years: None,
        };
        let bytes: Bytes = config.to_xml().into();
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));
        //TODO consider const body

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "object-lock"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body))
    }
}
