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

use minio::s3::response::{
    GenerateInventoryConfigYamlResponse, GetBucketInventoryConfigurationResponse,
    PutBucketInventoryConfigurationResponse,
};
use minio::s3::types::{BucketName, S3Api};
use minio_common::test_context::TestContext;

/// Bucket inventory configuration is a MinIO (AIStor) extension. Generates a
/// YAML template for an inventory config, stores it, then reads it back and
/// confirms the ID round-trips.
///
/// NOTE: The server may validate the inventory destination when storing the
/// config; this uses the server-generated template verbatim to avoid feeding
/// the server a YAML shape it would reject.
#[minio_macros::test]
async fn inventory(ctx: TestContext, bucket: BucketName) {
    if std::env::var("MINIO_AISTOR").is_err() {
        eprintln!(
            "skipping inventory: requires AIStor (set MINIO_AISTOR=1)"
        );
        return;
    }

    let id = "inv-test-1";

    let resp: GenerateInventoryConfigYamlResponse = ctx
        .client
        .generate_inventory_config_yaml(&bucket, id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let yaml_def = resp.yaml().unwrap().to_string();
    assert!(!yaml_def.is_empty(), "expected a non-empty YAML template");

    let _resp: PutBucketInventoryConfigurationResponse = ctx
        .client
        .put_bucket_inventory_configuration(&bucket, id, yaml_def)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let resp: GetBucketInventoryConfigurationResponse = ctx
        .client
        .get_bucket_inventory_configuration(&bucket, id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let config = resp.config().unwrap();
    assert_eq!(config.id, id);
}
