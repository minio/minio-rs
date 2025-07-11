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

use crate::s3::builders::{PutBucketLifecycle, PutBucketLifecycleBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`PutBucketLifecycle`] request builder.
    ///
    /// To execute the request, call [`SetBucketLifecycle::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketLifecycleResponse`](crate::s3::response::PutBucketLifecycleResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// use minio::s3::MinioClient;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::PutBucketLifecycleResponse;
    /// use minio::s3::types::{Filter, S3Api};
    /// use minio::s3::lifecycle_config::{LifecycleRule, LifecycleConfig};
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let rules: Vec<LifecycleRule> = vec![LifecycleRule {
    ///         id: String::from("rule1"),
    ///         filter: Filter {and_operator: None, prefix: Some(String::from("logs/")), tag: None},
    ///         expiration_days: Some(365),
    ///         status: true,
    ///         ..Default::default()
    ///     }];
    ///
    ///     let resp: PutBucketLifecycleResponse = client
    ///         .put_bucket_lifecycle("bucket-name")
    ///         .life_cycle_config(LifecycleConfig { rules })
    ///         .build().send().await.unwrap();
    ///     println!("set bucket replication policy on bucket '{}'", resp.bucket());
    /// }
    /// ```
    pub fn put_bucket_lifecycle<S: Into<String>>(&self, bucket: S) -> PutBucketLifecycleBldr {
        PutBucketLifecycle::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
