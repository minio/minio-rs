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

use minio::s3::inventory::{
    DestinationSpec, GetInventoryConfigResponse, JobDefinition, ModeSpec, OnOrOff, OutputFormat,
    PutInventoryConfigResponse, Schedule, VersionsSpec,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn put_and_get_inventory_config(ctx: TestContext, bucket_name: String) {
    let job_id = "test-put-get-job";
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Create job definition
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
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
    let _put_resp: PutInventoryConfigResponse = ctx
        .client
        .put_inventory_config(&bucket_name, job_id, job)
        .build()
        .send()
        .await
        .unwrap();

    // Get inventory config
    let get_resp: GetInventoryConfigResponse = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.bucket(), bucket_name);
    assert_eq!(get_resp.id(), job_id);
    assert!(!get_resp.user().is_empty(), "User should not be empty");
    assert!(
        !get_resp.yaml_definition().is_empty(),
        "YAML definition should not be empty"
    );

    let yaml = get_resp.yaml_definition();
    assert!(yaml.contains("apiVersion: v1"));
    assert!(yaml.contains(&format!("id: {job_id}")));
    assert!(yaml.contains(&format!("bucket: {dest_bucket}")));

    // Cleanup
    ctx.client
        .delete_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .ok();

    ctx.client
        .delete_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();
}
