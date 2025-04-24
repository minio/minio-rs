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
use crate::s3::types::{FromS3Response, NotificationConfig, S3Request};
use crate::s3::utils::take_bucket;
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_notification`](crate::s3::client::Client::get_bucket_notification) API call,
/// providing the notification configuration of an S3 bucket.
///
/// This configuration specifies the events for which Amazon S3 sends notifications and the destinations
/// (such as Amazon SNS topics, Amazon SQS queues, or AWS Lambda functions) where these notifications are sent.
///
/// For more information, refer to the [AWS S3 GetBucketNotificationConfiguration API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html).
#[derive(Clone, Debug)]
pub struct GetBucketNotificationResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose notification configuration is retrieved.
    pub bucket: String,

    /// The notification configuration of the bucket.
    ///
    /// This includes the event types and the destinations (e.g., SNS topics, SQS queues, Lambda functions)
    /// configured to receive notifications for those events.
    ///
    /// If the bucket has no notification configuration, this field may contain an empty configuration.
    pub config: NotificationConfig,
}

#[async_trait]
impl FromS3Response for GetBucketNotificationResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;
        let config = NotificationConfig::from_xml(&mut root)?;

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            config,
        })
    }
}
