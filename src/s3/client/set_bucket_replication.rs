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
use crate::s3::builders::SetBucketReplication;

impl Client {
    /// Returns argument for [set_bucket_replication()](crate::s3::client::Client::set_bucket_replication) API with given bucket name and configuration
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use minio::s3::args::*;
    /// use minio::s3::types::*;
    /// use std::collections::HashMap;
    /// let mut tags: HashMap<String, String> = HashMap::new();
    /// tags.insert(String::from("key1"), String::from("value1"));
    /// tags.insert(String::from("key2"), String::from("value2"));
    /// let mut rules: Vec<ReplicationRule> = Vec::new();
    /// rules.push(ReplicationRule {
    ///     destination: Destination {
    ///         bucket_arn: String::from("REPLACE-WITH-ACTUAL-DESTINATION-BUCKET-ARN"),
    ///         access_control_translation: None,
    ///         account: None,
    ///         encryption_config: None,
    ///         metrics: None,
    ///         replication_time: None,
    ///         storage_class: None,
    ///     },
    ///     delete_marker_replication_status: None,
    ///     existing_object_replication_status: None,
    ///     filter: Some(Filter {
    ///         and_operator: Some(AndOperator {
    ///     	    prefix: Some(String::from("TaxDocs")),
    ///     	    tags: Some(tags),
    ///         }),
    ///         prefix: None,
    ///         tag: None,
    ///     }),
    ///     id: Some(String::from("rule1")),
    ///     prefix: None,
    ///     priority: Some(1),
    ///     source_selection_criteria: None,
    ///     delete_replication_status: Some(false),
    ///     status: true,
    /// });
    /// let config = ReplicationConfig {role: None, rules: rules};
    /// let args = SetBucketReplicationArgs::new("my-bucket", &config).unwrap();
    /// ```
    pub fn set_bucket_replication(&self, bucket: &str) -> SetBucketReplication {
        SetBucketReplication::new(bucket).client(self)
    }
}
