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

use minio::admin::types::AdminApi;
use minio::s3::inventory::{
    AdminInventoryControlResponse, DestinationSpec, JobDefinition, JobState, ModeSpec, OnOrOff,
    OutputFormat, Schedule, VersionsSpec,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use std::time::Duration;

#[minio_macros::test(no_cleanup)]
async fn inventory_admin_suspend_resume(ctx: TestContext, bucket_name: String) {
    let job_id = "test-admin-suspend-resume";
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Create inventory job with recurring schedule
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
            prefix: Some("admin-test/".to_string()),
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Daily,
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

    let admin = ctx.client.admin();

    // Suspend the job
    let suspend_resp: AdminInventoryControlResponse = admin
        .suspend_inventory_job(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(suspend_resp.status(), "suspended");
    assert_eq!(suspend_resp.bucket(), bucket_name);
    assert_eq!(suspend_resp.inventory_id(), job_id);

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify job is suspended
    let status = ctx
        .client
        .get_inventory_job_status(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        status.state(),
        JobState::Suspended,
        "Job should be suspended"
    );

    // Resume the job
    let resume_resp: AdminInventoryControlResponse = admin
        .resume_inventory_job(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resume_resp.status(), "resumed");
    assert_eq!(resume_resp.bucket(), bucket_name);
    assert_eq!(resume_resp.inventory_id(), job_id);

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify job is no longer suspended
    let status = ctx
        .client
        .get_inventory_job_status(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();
    assert_ne!(
        status.state(),
        JobState::Suspended,
        "Job should not be suspended after resume"
    );

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

#[minio_macros::test(no_cleanup)]
async fn inventory_admin_cancel(ctx: TestContext, bucket_name: String) {
    let job_id = "test-admin-cancel";
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
            prefix: Some("cancel-test/".to_string()),
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

    let admin = ctx.client.admin();

    // Cancel the job
    let cancel_resp: AdminInventoryControlResponse = admin
        .cancel_inventory_job(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(cancel_resp.status(), "canceled");
    assert_eq!(cancel_resp.bucket(), bucket_name);
    assert_eq!(cancel_resp.inventory_id(), job_id);

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify job is canceled
    let status = ctx
        .client
        .get_inventory_job_status(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(status.state(), JobState::Canceled, "Job should be canceled");

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
