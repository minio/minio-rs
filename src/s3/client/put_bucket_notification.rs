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

use crate::s3::builders::{PutBucketNotification, PutBucketNotificationBldr};
use crate::s3::client::MinioClient;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`PutBucketNotification`] request builder.
    ///
    /// To execute the request, call [`SetBucketNotification::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketNotificationResponse`](crate::s3::response::PutBucketNotificationResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::{BucketName, NotificationConfig, PrefixFilterRule, QueueConfig, S3Api, SuffixFilterRule};
    /// use minio::s3::response::PutBucketNotificationResponse;
    /// use minio::s3::response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let config = NotificationConfig {
    ///         cloud_func_config_list: None,
    ///         queue_config_list: Some(vec![QueueConfig {
    ///             events: vec![
    ///                 String::from("s3:ObjectCreated:Put"),
    ///                 String::from("s3:ObjectCreated:Copy"),
    ///             ],
    ///             id: None,
    ///             prefix_filter_rule: Some(PrefixFilterRule {
    ///                 value: String::from("images"),
    ///             }),
    ///             suffix_filter_rule: Some(SuffixFilterRule {
    ///                 value: String::from("pg"),
    ///             }),
    ///             queue: String::from("arn:minio:sqs:us-east-1:miniojavatest:webhook"),
    ///         }]),
    ///         topic_config_list: None,
    ///     };
    ///
    ///     let resp: PutBucketNotificationResponse = client
    ///         .put_bucket_notification(BucketName::new("bucket-name").unwrap())
    ///         .notification_config(config)
    ///         .build().send().await.unwrap();
    ///     println!("set bucket notification for bucket '{:?}'", resp.bucket());
    /// }
    /// ```
    pub fn put_bucket_notification(&self, bucket: BucketName) -> PutBucketNotificationBldr {
        PutBucketNotification::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
