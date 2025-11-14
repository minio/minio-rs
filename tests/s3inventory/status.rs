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
    DestinationSpec, GetInventoryJobStatusResponse, JobDefinition, JobState, JobStatus, ModeSpec,
    OnOrOff, OutputFormat, Schedule, VersionsSpec,
};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn get_inventory_job_status(ctx: TestContext, bucket_name: BucketName) {
    let job_id = "test-status-job";
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

    // Create inventory job
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
            prefix: Some("status-test/".to_string()),
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
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Get job status
    let resp: GetInventoryJobStatusResponse = ctx
        .client
        .get_inventory_job_status(&bucket_name, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let status: JobStatus = resp.status().unwrap();

    // Verify status fields
    assert_eq!(status.bucket, bucket_name.as_str());
    assert_eq!(status.id, job_id);
    assert!(!status.user.is_empty(), "User should not be empty");
    assert!(
        !status.access_key.is_empty(),
        "Access key should not be empty"
    );
    assert_eq!(status.schedule, Schedule::Once);

    // Verify state is valid
    let valid_states = [
        JobState::Sleeping,
        JobState::Pending,
        JobState::Running,
        JobState::Completed,
        JobState::Errored,
        JobState::Suspended,
        JobState::Canceled,
        JobState::Failed,
    ];
    assert!(
        valid_states.contains(&status.state),
        "Job state should be valid: {:?}",
        status.state
    );

    // Verify response helper methods
    assert_eq!(resp.bucket().unwrap(), &bucket_name);
    assert_eq!(status.id, job_id);

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
