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

use crate::s3::builders::{PutBucketReplication, PutBucketReplicationBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`PutBucketReplication`] request builder.
    ///
    /// To execute the request, call [`SetBucketReplication::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketReplicationResponse`](crate::s3::response::PutBucketReplicationResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::PutBucketReplicationResponse;
    /// use minio::s3::types::{S3Api, AndOperator, Destination, Filter, ReplicationConfig, ReplicationRule};
    /// use minio::s3::response::a_response_traits::HasBucket;
    /// use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     
    ///     let mut tags: HashMap<String, String> = HashMap::new();  
    ///     tags.insert(String::from("key1"), String::from("value1"));
    ///     tags.insert(String::from("key2"), String::from("value2"));
    ///
    ///     let mut rules: Vec<ReplicationRule> = Vec::new();
    ///     rules.push(ReplicationRule {
    ///         destination: Destination {
    ///             bucket_arn: String::from("REPLACE-WITH-ACTUAL-DESTINATION-BUCKET-ARN"),
    ///             access_control_translation: None,
    ///             account: None,
    ///             encryption_config: None,
    ///             metrics: None,
    ///             replication_time: None,
    ///             storage_class: None,
    ///         },
    ///         delete_marker_replication_status: None,
    ///         existing_object_replication_status: None,
    ///         filter: Some(Filter {
    ///             and_operator: Some(AndOperator {
    ///                 prefix: Some(String::from("TaxDocs")),
    ///                 tags: Some(tags),
    ///             }),
    ///             prefix: None,
    ///             tag: None,
    ///         }),
    ///         id: Some(String::from("rule1")),
    ///         prefix: None,
    ///         priority: Some(1),
    ///         source_selection_criteria: None,
    ///         delete_replication_status: Some(false),
    ///         status: true,
    ///     });
    ///
    ///     let resp: PutBucketReplicationResponse = client
    ///         .put_bucket_replication("bucket-name")
    ///         .replication_config(ReplicationConfig {role: None, rules})
    ///         .build().send().await.unwrap();
    ///     println!("enabled versioning on bucket '{}'", resp.bucket());
    /// }
    /// ```
    pub fn put_bucket_replication<S: Into<String>>(&self, bucket: S) -> PutBucketReplicationBldr {
        PutBucketReplication::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
