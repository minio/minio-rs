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

//! S3 APIs for bucket objects.

use super::Client;
use crate::s3::builders::SetBucketNotification;

impl Client {
    /// Create a SetBucketNotification request builder.
    ///
    /// Returns argument for [set_bucket_notification()](crate::s3::client::Client::set_bucket_notification) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use minio::s3::Client;
    /// use minio::s3::types::{NotificationConfig, PrefixFilterRule, QueueConfig, S3Api, SuffixFilterRule};
    ///
    /// #[tokio::main]
    /// async fn main() {
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
    ///             queue: String::from("arn:minio:sqs::miniojavatest:webhook"),
    ///         }]),
    ///         topic_config_list: None,
    ///     };
    ///     let client = Client::default();
    ///     let _resp = client
    ///          .set_bucket_notification("my-bucket-name")
    ///          .notification_config(config)
    ///          .send().await;
    /// }
    /// ```
    pub fn set_bucket_notification(&self, bucket: &str) -> SetBucketNotification {
        SetBucketNotification::new(bucket).client(self)
    }
}
