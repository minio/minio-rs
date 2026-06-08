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

use minio::s3::builders::{QOSConfig, QOSRule, QOS_CONFIG_VERSION_CURRENT};
use minio::s3::response::{GetBucketQOSResponse, SetBucketQOSResponse};
use minio::s3::types::{BucketName, S3Api};
use minio_common::test_context::TestContext;

/// Bucket QoS is a MinIO (AIStor) extension. Sets a QoS configuration with one
/// rule, then reads it back and confirms the rule round-trips.
#[minio_macros::test]
async fn bucket_qos(ctx: TestContext, bucket: BucketName) {
    if std::env::var("MINIO_AISTOR").is_err() {
        eprintln!(
            "skipping bucket_qos: requires AIStor (set MINIO_AISTOR=1)"
        );
        return;
    }

    let config = QOSConfig {
        version: QOS_CONFIG_VERSION_CURRENT.to_string(),
        rules: vec![QOSRule {
            id: "rule-1".to_string(),
            label: "ingest".to_string(),
            priority: 10,
            object_prefix: "logs/".to_string(),
            api: "PutObject".to_string(),
            rate: 100,
            burst: 200,
            limit: "rps".to_string(),
        }],
    };

    let _resp: SetBucketQOSResponse = ctx
        .client
        .set_bucket_qos(&bucket)
        .unwrap()
        .qos_config(config.clone())
        .build()
        .send()
        .await
        .unwrap();

    let resp: GetBucketQOSResponse = ctx
        .client
        .get_bucket_qos(&bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let got = resp.config().unwrap();
    assert_eq!(got.rules.len(), 1);
    assert_eq!(got.rules[0].id, "rule-1");
    assert_eq!(got.rules[0].object_prefix, "logs/");
    assert_eq!(got.rules[0].api, "PutObject");
}
