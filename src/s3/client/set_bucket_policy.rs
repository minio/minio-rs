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
use crate::s3::builders::SetBucketPolicy;

impl Client {
    /// Create a SetBucketPolicy request builder.
    ///
    /// Returns argument for [set_bucket_policy()](crate::s3::client::Client::set_bucket_policy) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use minio::s3::Client;
    /// use minio::s3::types::{Filter, LifecycleConfig, LifecycleRule, S3Api};
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let config = r#"{
    ///   "Version": "2012-10-17",
    ///   "Statement": [
    ///     {
    ///       "Effect": "Allow",
    ///       "Principal": {
    ///         "AWS": "*"
    ///       },
    ///       "Action": [
    ///         "s3:GetBucketLocation",
    ///         "s3:ListBucket"
    ///       ],
    ///       "Resource": "arn:aws:s3:::my-bucket"
    ///     },
    ///     {
    ///       "Effect": "Allow",
    ///       "Principal": {
    ///         "AWS": "*"
    ///       },
    ///       "Action": "s3:GetObject",
    ///       "Resource": "arn:aws:s3:::my-bucket-name/*"
    ///     }
    ///   ]
    /// }"#;
    ///     let client = Client::default();
    ///     let _resp = client
    ///          .set_bucket_policy("my-bucket-name")
    ///          .config(config).
    ///          .send().await.unwrap();
    /// }
    /// ```
    pub fn set_bucket_policy(&self, bucket: &str) -> SetBucketPolicy {
        SetBucketPolicy::new(bucket).client(self)
    }
}
