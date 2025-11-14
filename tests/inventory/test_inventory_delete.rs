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
    DeleteInventoryConfigResponse, DestinationSpec, JobDefinition, ModeSpec, OnOrOff, OutputFormat,
    Schedule, VersionsSpec,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn delete_inventory_config(ctx: TestContext, bucket_name: String) {
    let job_id = "test-delete-job";
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Create inventory job
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
            prefix: Some("reports/".to_string()),
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

    ctx.client
        .put_inventory_config(&bucket_name, job_id, job)
        .build()
        .send()
        .await
        .unwrap();

    // Verify job exists
    let get_result = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await;
    assert!(get_result.is_ok(), "Job should exist before deletion");

    // Delete inventory config
    let _delete_resp: DeleteInventoryConfigResponse = ctx
        .client
        .delete_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    // Verify job no longer exists
    let get_result = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await;
    assert!(get_result.is_err(), "Job should not exist after deletion");

    // Cleanup
    ctx.client
        .delete_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();
}
