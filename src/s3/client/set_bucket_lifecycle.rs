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
use crate::s3::builders::SetBucketLifecycle;

impl Client {
    /// Create a SetBucketLifecycle request builder.
    ///
    /// Returns argument for [set_bucket_lifecycle()](crate::s3::client::Client::set_bucket_lifecycle) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use minio::s3::Client;
    /// use minio::s3::types::{Filter, LifecycleConfig, LifecycleRule, S3Api};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let rules: Vec<LifecycleRule> = vec![LifecycleRule {
    ///         abort_incomplete_multipart_upload_days_after_initiation: None,
    ///         expiration_date: None,
    ///         expiration_days: Some(365),
    ///         expiration_expired_object_delete_marker: None,
    ///         filter: Filter {and_operator: None, prefix: Some(String::from("logs/")), tag: None},
    ///         id: String::from("rule1"),
    ///         noncurrent_version_expiration_noncurrent_days: None,
    ///         noncurrent_version_transition_noncurrent_days: None,
    ///         noncurrent_version_transition_storage_class: None,
    ///         status: true,
    ///         transition_date: None,
    ///         transition_days: None,
    ///         transition_storage_class: None,
    ///     }];
    ///     let client = Client::default();
    ///     let _resp = client
    ///          .set_bucket_lifecycle("my-bucket-name")
    ///          .life_cycle_config(LifecycleConfig { rules })
    ///          .send().await;
    /// }
    /// ```
    pub fn set_bucket_lifecycle(&self, bucket: &str) -> SetBucketLifecycle {
        SetBucketLifecycle::new(bucket).client(self)
    }
}
