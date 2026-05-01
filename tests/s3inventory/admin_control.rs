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

use minio::madmin::MinioAdminClient;
use minio::madmin::types::MadminApi;
use minio::madmin::{AdminControlJson, AdminControlStatus, AdminInventoryControlResponse};
use minio::s3::types::{BucketName, S3Api};
use minio::s3inventory::{
    DestinationSpec, GetInventoryJobStatusResponse, JobDefinition, JobState, JobStatus, ModeSpec,
    OnOrOff, OutputFormat, Schedule, VersionsSpec,
};
use minio_common::test_context::TestContext;
use std::time::Duration;

#[minio_macros::test(no_cleanup)]
async fn inventory_admin_suspend_resume(ctx: TestContext, bucket_name: BucketName) {
    let job_id = "test-admin-suspend-resume";
    let dest_bucket_str = format!("{bucket_name}-dest");
    let bucket = bucket_name.clone();
    let dest_bucket = BucketName::new(&dest_bucket_str).unwrap();

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();

    // Create inventory job with recurring schedule
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
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
        .put_inventory_config(&bucket, job_id, job)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let admin: MinioAdminClient = ctx.client.admin();

    // Suspend the job
    let resp: AdminInventoryControlResponse = admin
        .suspend_inventory_job(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let control: AdminControlJson = resp.admin_control().unwrap();
    assert_eq!(control.status, AdminControlStatus::Suspended);
    assert_eq!(control.bucket, bucket_name.as_str());
    assert_eq!(control.inventory_id, job_id);

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify job is suspended
    let resp: GetInventoryJobStatusResponse = ctx
        .client
        .get_inventory_job_status(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().unwrap().state,
        JobState::Suspended,
        "Job should be suspended"
    );

    // Resume the job
    let resp: AdminInventoryControlResponse = admin
        .resume_inventory_job(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let control: AdminControlJson = resp.admin_control().unwrap();
    assert_eq!(control.status, AdminControlStatus::Resumed);
    assert_eq!(control.bucket, bucket_name.as_str());
    assert_eq!(control.inventory_id, job_id);

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify job is no longer suspended
    let resp: GetInventoryJobStatusResponse = ctx
        .client
        .get_inventory_job_status(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_ne!(
        resp.status().unwrap().state,
        JobState::Suspended,
        "Job should not be suspended after resume"
    );

    // Cleanup
    ctx.client
        .delete_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .ok();

    ctx.client
        .delete_bucket(dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();
}

#[minio_macros::test(no_cleanup)]
async fn inventory_admin_cancel(ctx: TestContext, bucket_name: BucketName) {
    let job_id = "test-admin-cancel";
    let dest_bucket_str = format!("{bucket_name}-dest");
    let bucket = bucket_name.clone();
    let dest_bucket = BucketName::new(&dest_bucket_str).unwrap();

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();

    // Create inventory job
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
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
        .put_inventory_config(&bucket, job_id, job)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let admin = ctx.client.admin();

    // Cancel the job
    let resp: AdminInventoryControlResponse = admin
        .cancel_inventory_job(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let control: AdminControlJson = resp.admin_control().unwrap();
    assert_eq!(control.status, AdminControlStatus::Canceled);
    assert_eq!(control.bucket, bucket_name.as_str());
    assert_eq!(control.inventory_id, job_id);

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify job is canceled
    let resp: GetInventoryJobStatusResponse = ctx
        .client
        .get_inventory_job_status(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let status: JobStatus = resp.status().unwrap();
    assert_eq!(status.state, JobState::Canceled, "Job should be canceled");

    // Cleanup
    ctx.client
        .delete_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .ok();

    ctx.client
        .delete_bucket(dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();
}
