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
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, NotificationConfig, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_notification`](crate::s3::client::MinioClient::get_bucket_notification) API call,
/// providing the notification configuration of an S3 bucket.
///
/// This configuration specifies the events for which Amazon S3 sends notifications and the destinations
/// (such as Amazon SNS topics, Amazon SQS queues, or AWS Lambda functions) where these notifications are sent.
///
/// For more information, refer to the [AWS S3 GetBucketNotificationConfiguration API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html).
#[derive(Clone, Debug)]
pub struct GetBucketNotificationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetBucketNotificationResponse);
impl_has_s3fields!(GetBucketNotificationResponse);

impl HasBucket for GetBucketNotificationResponse {}
impl HasRegion for GetBucketNotificationResponse {}

impl GetBucketNotificationResponse {
    /// Returns the notification configuration of the bucket.
    ///
    /// This configuration includes the event types and the destinations (e.g., SNS topics, SQS queues, Lambda functions)
    /// configured to receive notifications for those events.
    pub fn config(&self) -> Result<NotificationConfig, ValidationErr> {
        NotificationConfig::from_xml(&mut Element::parse(self.body.clone().reader())?)
    }
}
