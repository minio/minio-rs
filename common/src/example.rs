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

use chrono::{DateTime, Utc};
use minio::s3::builders::PostPolicy;
use minio::s3::lifecycle_config::{LifecycleConfig, LifecycleRule};
use minio::s3::types::{
    AndOperator, CsvInputSerialization, CsvOutputSerialization, Destination, FileHeaderInfo,
    Filter, NotificationConfig, ObjectLockConfig, PrefixFilterRule, QueueConfig, QuoteFields,
    ReplicationConfig, ReplicationRule, RetentionMode, SelectRequest, SuffixFilterRule,
};
use minio::s3::utils::utc_now;
use std::collections::HashMap;

pub fn create_bucket_lifecycle_config_examples() -> LifecycleConfig {
    LifecycleConfig {
        rules: vec![LifecycleRule {
            id: String::from("rule1"),
            expiration_days: Some(365),
            filter: Filter {
                prefix: Some(String::from("logs/")),
                ..Default::default()
            },
            status: true,
            ..Default::default()
        }],
    }
}
pub fn create_bucket_notification_config_example() -> NotificationConfig {
    NotificationConfig {
        queue_config_list: Some(vec![QueueConfig {
            events: vec![
                String::from("s3:ObjectCreated:Put"),
                String::from("s3:ObjectCreated:Copy"),
            ],
            id: Some("".to_string()), //TODO or should this be NONE??
            prefix_filter_rule: Some(PrefixFilterRule {
                value: String::from("images"),
            }),
            suffix_filter_rule: Some(SuffixFilterRule {
                value: String::from("pg"),
            }),
            queue: String::from("arn:minio:sqs::miniojavatest:webhook"),
        }]),
        ..Default::default()
    }
}
pub fn create_bucket_policy_config_example(bucket_name: &str) -> String {
    let config = r#"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::<BUCKET>/myobject*"
            ],
            "Sid": ""
        }
    ]
}
"#
    .replace("<BUCKET>", bucket_name);
    config.to_string()
}
pub fn create_bucket_policy_config_example_for_replication() -> String {
    let config = r#"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetReplicationConfiguration",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:GetBucketLocation",
                "s3:GetBucketVersioning",
                "s3:GetBucketObjectLockConfiguration",
                "s3:GetEncryptionConfiguration"
            ],
            "Resource": [
                "arn:aws:s3:::*"
            ],
            "Sid": "EnableReplicationOnBucket"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetReplicationConfiguration",
                "s3:ReplicateTags",
                "s3:AbortMultipartUpload",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:GetObjectVersionTagging",
                "s3:PutObject",
                "s3:PutObjectRetention",
                "s3:PutBucketObjectLockConfiguration",
                "s3:PutObjectLegalHold",
                "s3:DeleteObject",
                "s3:ReplicateObject",
                "s3:ReplicateDelete"
            ],
            "Resource": [
                "arn:aws:s3:::*"
            ],
            "Sid": "EnableReplicatingDataIntoBucket"
        }
    ]
}"#;
    config.to_string()
}
pub fn create_bucket_replication_config_example(dst_bucket: &str) -> ReplicationConfig {
    let mut tags: HashMap<String, String> = HashMap::new();
    tags.insert(String::from("key1"), String::from("value1"));
    tags.insert(String::from("key2"), String::from("value2"));

    ReplicationConfig {
        role: Some("example1".to_string()),
        rules: vec![ReplicationRule {
            id: Some(String::from("rule1")),
            destination: Destination {
                bucket_arn: String::from(&format!("arn:aws:s3:::{}", dst_bucket)),
                ..Default::default()
            },
            filter: Some(Filter {
                and_operator: Some(AndOperator {
                    prefix: Some(String::from("TaxDocs")),
                    tags: Some(tags),
                }),
                ..Default::default()
            }),
            priority: Some(1),
            delete_replication_status: Some(false),
            status: true,
            ..Default::default()
        }],
    }
}
pub fn create_tags_example() -> HashMap<String, String> {
    HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ])
}
pub fn create_object_lock_config_example() -> ObjectLockConfig {
    const DURATION_DAYS: i32 = 7;
    ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(DURATION_DAYS), None).unwrap()
}
pub fn create_post_policy_example(bucket_name: &str, object_name: &str) -> PostPolicy {
    let expiration: DateTime<Utc> = utc_now() + chrono::Duration::days(5);

    let mut policy = PostPolicy::new(&bucket_name, expiration).unwrap();
    policy.add_equals_condition("key", &object_name).unwrap();
    policy
        .add_content_length_range_condition(1024 * 1024, 4 * 1024 * 1024)
        .unwrap();
    policy
}
/// return (body, data)
pub fn create_select_content_data() -> (String, String) {
    let mut data = String::new();
    data.push_str("1997,Ford,E350,\"ac, abs, moon\",3000.00\n");
    data.push_str("1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00\n");
    data.push_str("1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n");
    data.push_str("1996,Jeep,Grand Cherokee,\"MUST SELL!\n");
    data.push_str("air, moon roof, loaded\",4799.00\n");
    let body = String::from("Year,Make,Model,Description,Price\n") + &data;
    (body, data)
}
pub fn create_select_content_request() -> SelectRequest {
    let request = SelectRequest::new_csv_input_output(
        "select * from S3Object",
        CsvInputSerialization {
            compression_type: None,
            allow_quoted_record_delimiter: false,
            comments: None,
            field_delimiter: None,
            file_header_info: Some(FileHeaderInfo::USE),
            quote_character: None,
            quote_escape_character: None,
            record_delimiter: None,
        },
        CsvOutputSerialization {
            field_delimiter: None,
            quote_character: None,
            quote_escape_character: None,
            quote_fields: Some(QuoteFields::ASNEEDED),
            record_delimiter: None,
        },
    )
    .unwrap();
    request
}
