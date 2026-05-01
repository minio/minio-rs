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

use minio::s3::response_traits::HasBucket;
use minio::s3::types::{BucketName, S3Api};
use minio::s3inventory::{
    DestinationSpec, GetInventoryConfigJson, GetInventoryConfigResponse, JobDefinition, ModeSpec,
    OnOrOff, OutputFormat, PutInventoryConfigResponse, Schedule, VersionsSpec,
};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn put_and_get_inventory_config(ctx: TestContext, bucket_name: BucketName) {
    let job_id = "test-put-get-job";
    let dest_bucket_str = format!("{bucket_name}-dest");
    let dest_bucket = BucketName::new(&dest_bucket_str).unwrap();

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();

    // Create job definition
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
            prefix: Some("inventory-reports/".to_string()),
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Once,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    // Put inventory config
    let _resp: PutInventoryConfigResponse = ctx
        .client
        .put_inventory_config(&bucket_name, job_id, job)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Get inventory config
    let resp: GetInventoryConfigResponse = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let config: GetInventoryConfigJson = resp.inventory_config().unwrap();

    assert_eq!(resp.bucket().unwrap(), &bucket_name);
    assert_eq!(config.id, job_id);
    assert!(!config.user.is_empty(), "User should not be empty");
    assert!(
        !config.yaml_def.is_empty(),
        "YAML definition should not be empty"
    );

    let yaml: &str = &config.yaml_def;
    assert!(yaml.contains("apiVersion: v1"));
    assert!(yaml.contains(&format!("id: {job_id}")));
    assert!(yaml.contains(&format!("bucket: {dest_bucket_str}")));

    // Cleanup
    ctx.client
        .delete_inventory_config(&bucket_name, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .ok();

    ctx.client
        .delete_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();
}
