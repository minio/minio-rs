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
use crate::s3::response::DeleteBucketNotificationResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{NotificationConfig, S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, insert};
use bytes::Bytes;
use http::Method;

/// Argument builder for [delete_bucket_notification()](crate::s3::client::Client::delete_bucket_notification) API
pub type DeleteBucketNotification = BucketCommon<DeleteBucketNotificationPhantomData>;

#[derive(Default, Debug)]
pub struct DeleteBucketNotificationPhantomData;

impl S3Api for DeleteBucketNotification {
    type S3Response = DeleteBucketNotificationResponse;
}

impl ToS3Request for DeleteBucketNotification {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        const CONFIG: NotificationConfig = NotificationConfig {
            cloud_func_config_list: None,
            queue_config_list: None,
            topic_config_list: None,
        };
        let bytes: Bytes = CONFIG.to_xml().into();
        let body: Option<SegmentedBytes> = Some(SegmentedBytes::from(bytes));
        //TODO consider const body

        Ok(S3Request::new(self.client, Method::PUT)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "notification"))
            .headers(self.extra_headers.unwrap_or_default())
            .body(body))
    }
}
