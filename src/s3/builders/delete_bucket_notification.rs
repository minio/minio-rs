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
use crate::s3::response::DeleteBucketNotificationResponse;
use crate::s3::types::{NotificationConfig, S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use bytes::Bytes;
use http::Method;

/// Argument builder for [delete_bucket_notification()](Client::delete_bucket_notification) API
pub type DeleteBucketNotification = BucketCommon<DeleteBucketNotificationPhantomData>;

#[derive(Default, Debug)]
pub struct DeleteBucketNotificationPhantomData;

impl S3Api for DeleteBucketNotification {
    type S3Response = DeleteBucketNotificationResponse;
}

impl ToS3Request for DeleteBucketNotification {
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

        query_params.insert(String::from("notification"), String::new());

        const CONFIG: NotificationConfig = NotificationConfig {
            cloud_func_config_list: None,
            queue_config_list: None,
            topic_config_list: None,
        };
        let bytes: Bytes = CONFIG.to_xml().into();
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
