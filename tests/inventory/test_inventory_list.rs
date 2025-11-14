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
    DestinationSpec, JobDefinition, ListInventoryConfigsResponse, ModeSpec, OnOrOff, OutputFormat,
    Schedule, VersionsSpec,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn list_inventory_configs(ctx: TestContext, bucket_name: String) {
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Create multiple inventory jobs
    let job_ids = vec!["test-list-job-1", "test-list-job-2", "test-list-job-3"];

    for job_id in &job_ids {
        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: job_id.to_string(),
            destination: DestinationSpec {
                bucket: dest_bucket.clone(),
                prefix: Some(format!("{job_id}/")),
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
            .put_inventory_config(&bucket_name, *job_id, job)
            .build()
            .send()
            .await
            .unwrap();
    }

    // List inventory configs
    let list_resp: ListInventoryConfigsResponse = ctx
        .client
        .list_inventory_configs(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();

    let items = list_resp.items();
    assert!(
        items.len() >= 3,
        "Should have at least 3 inventory jobs, got {}",
        items.len()
    );

    // Verify all created jobs are in the list
    for job_id in &job_ids {
        let found = items.iter().any(|item| item.id == *job_id);
        assert!(found, "Job {job_id} should be in the list");
    }

    // Verify items have required fields
    for item in items {
        assert_eq!(item.bucket, bucket_name);
        assert!(!item.id.is_empty());
        assert!(!item.user.is_empty());
    }

    // Cleanup
    for job_id in &job_ids {
        ctx.client
            .delete_inventory_config(&bucket_name, *job_id)
            .build()
            .send()
            .await
            .ok();
    }

    ctx.client
        .delete_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();
}
