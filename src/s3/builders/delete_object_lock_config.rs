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
use crate::s3::builders::{BucketCommon, SegmentedBytes};
use crate::s3::error::Error;
use crate::s3::response::DeleteObjectLockConfigResponse;
use crate::s3::types::{ObjectLockConfig, S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use bytes::Bytes;
use http::Method;

/// Argument builder for [delete_object_lock_config()](Client::delete_object_lock_config) API
pub type DeleteObjectLockConfig = BucketCommon<DeleteObjectLockConfigPhantomData>;

#[derive(Default, Debug)]
pub struct DeleteObjectLockConfigPhantomData;

impl S3Api for DeleteObjectLockConfig {
    type S3Response = DeleteObjectLockConfigResponse;
}

impl ToS3Request for DeleteObjectLockConfig {
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

        query_params.insert(String::from("object-lock"), String::new());

        let config = ObjectLockConfig {
            retention_mode: None,
            retention_duration_days: None,
            retention_duration_years: None,
        };
        let bytes: Bytes = config.to_xml().into();
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));
        //TODO consider const body

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
