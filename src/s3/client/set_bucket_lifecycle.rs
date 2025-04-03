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
use std::sync::Arc;

impl Client {
    /// Creates a [`SetBucketLifecycle`] request builder.
    ///
    /// To execute the request, call [`SetBucketLifecycle::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketLifecycleResponse`](crate::s3::response::SetBucketLifecycleResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::SetBucketLifecycleResponse;
    /// use minio::s3::types::{Filter, LifecycleConfig, LifecycleRule, S3Api};
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///
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
    ///
    ///     let resp: SetBucketLifecycleResponse = client
    ///         .set_bucket_lifecycle("bucket-name")
    ///         .life_cycle_config(LifecycleConfig { rules })
    ///         .send().await.unwrap();
    ///     println!("set bucket replication policy on bucket '{}'", resp.bucket);
    /// }
    /// ```
    pub fn set_bucket_lifecycle(self: &Arc<Self>, bucket: &str) -> SetBucketLifecycle {
        SetBucketLifecycle::new(self, bucket.to_owned())
    }
}
