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
use minio::s3::client::DEFAULT_REGION;
use minio::s3::error::Error;
use minio::s3::response::{CreateBucketResponse, DeleteBucketResponse};
use minio::s3::response_traits::{HasBucket, HasRegion};
use minio::s3::types::{BucketName, S3Api};
use minio::s3inventory::{
    DeleteInventoryConfigResponse, DestinationSpec, FilterSpec, GenerateInventoryConfigResponse,
    GetInventoryConfigJson, GetInventoryConfigResponse, GetInventoryJobStatusResponse,
    JobDefinition, JobStatus, ListInventoryConfigsJson, ListInventoryConfigsResponse, ModeSpec,
    NameFilter, OnOrOff, OutputFormat, PutInventoryConfigResponse, Schedule, SizeFilter,
    VersionsSpec,
};
use minio_common::test_context::TestContext;
use std::time::Duration;

#[minio_macros::test(no_cleanup)]
async fn inventory_complete_workflow(ctx: TestContext, bucket_name: BucketName) {
    let job_id = "integration-test-job";
    let dest_bucket_str = format!("{bucket_name}-reports");
    let bucket = bucket_name.clone();
    let dest_bucket = BucketName::new(&dest_bucket_str).unwrap();

    let resp: CreateBucketResponse = ctx
        .client
        .create_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), dest_bucket_str);
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Step 2: Generate template (optional but shows API usage)
    let resp: GenerateInventoryConfigResponse = ctx
        .client
        .generate_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let yaml: String = resp.yaml_template().unwrap();
    assert!(!yaml.is_empty(), "Template should be generated");

    // Step 3: Create a filtered inventory job
    let filters = FilterSpec {
        prefix: Some(vec!["data/".to_string()]),
        size: Some(SizeFilter {
            less_than: Some("10GiB".to_string()),
            greater_than: Some("1B".to_string()),
            equal_to: None,
        }),
        name: Some(vec![NameFilter {
            match_pattern: Some("*".to_string()),
            contains: None,
            regex: None,
        }]),
        last_modified: None,
        versions_count: None,
        tags: None,
        user_metadata: None,
    };

    // Create inventory job
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
            prefix: Some("inventory/".to_string()),
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Daily,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: Some(filters),
    };

    let resp: PutInventoryConfigResponse = ctx
        .client
        .put_inventory_config(&bucket, job_id, job)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), bucket_name.as_str());
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Step 4: Verify job was created by listing
    let resp: ListInventoryConfigsResponse = ctx
        .client
        .list_inventory_configs(&bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let configs: ListInventoryConfigsJson = resp.configs().unwrap();
    let found = configs.items.iter().any(|item| item.id == job_id);
    assert!(found, "Job should appear in list");

    // Step 5: Get job configuration
    let resp: GetInventoryConfigResponse = ctx
        .client
        .get_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let config: GetInventoryConfigJson = resp.inventory_config().unwrap();

    assert_eq!(config.id, job_id);
    assert!(!config.yaml_def.is_empty());

    // Step 6: Get job status
    let resp: GetInventoryJobStatusResponse = ctx
        .client
        .get_inventory_job_status(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let status: JobStatus = resp.status().unwrap();
    assert_eq!(status.id, job_id);
    assert_eq!(resp.bucket().unwrap().as_str(), bucket_name.as_str());

    // Step 7: Test admin operations
    let admin: MinioAdminClient = ctx.client.admin();

    // Suspend
    let resp: AdminInventoryControlResponse = admin
        .suspend_inventory_job(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let control: AdminControlJson = resp.admin_control().unwrap();
    assert_eq!(control.status, AdminControlStatus::Suspended);

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Resume
    let resp: AdminInventoryControlResponse = admin
        .resume_inventory_job(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    let control: AdminControlJson = resp.admin_control().unwrap();
    assert_eq!(control.status, AdminControlStatus::Resumed);

    // Step 8: Update job configuration (via put with same ID)
    let updated_job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
            prefix: Some("updated-inventory/".to_string()),
            format: OutputFormat::JSON, // Changed format
            compression: OnOrOff::Off,  // Changed compression
            max_file_size_hint: None,
        },
        schedule: Schedule::Weekly,  // Changed schedule
        mode: ModeSpec::Strict,      // Changed mode
        versions: VersionsSpec::All, // Changed versions
        include_fields: vec![],
        filters: None, // Removed filters
    };

    let resp: PutInventoryConfigResponse = ctx
        .client
        .put_inventory_config(&bucket, job_id, updated_job)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), bucket_name.as_str());

    // Verify update
    let resp: GetInventoryConfigResponse = ctx
        .client
        .get_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let config: GetInventoryConfigJson = resp.inventory_config().unwrap();
    let yaml: &str = &config.yaml_def;
    assert!(yaml.contains("format: json"), "Should have JSON format");
    assert!(
        yaml.contains("schedule: weekly"),
        "Should have weekly schedule"
    );
    assert!(yaml.contains("mode: strict"), "Should have strict mode");
    assert!(yaml.contains("versions: all"), "Should have all versions");
    assert!(
        yaml.contains("compression: off"),
        "Should have compression off"
    );

    // Step 9: Delete job
    let resp: DeleteInventoryConfigResponse = ctx
        .client
        .delete_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), bucket_name.as_str());

    // Verify deletion
    let resp: Result<GetInventoryConfigResponse, Error> = ctx
        .client
        .get_inventory_config(bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await;

    assert!(resp.is_err(), "Job should not exist after deletion");

    // Cleanup
    let resp: DeleteBucketResponse = ctx
        .client
        .delete_bucket(dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), dest_bucket_str);
}

#[minio_macros::test(no_cleanup)]
async fn inventory_pagination_test(ctx: TestContext, bucket_name: BucketName) {
    let dest_bucket_str = format!("{bucket_name}-dest");
    let bucket = bucket_name.clone();
    let dest_bucket = BucketName::new(&dest_bucket_str).unwrap();

    let resp: CreateBucketResponse = ctx
        .client
        .create_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), dest_bucket_str);
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Create multiple jobs to test pagination
    let job_count = 5;
    for i in 0..job_count {
        let job_id = format!("pagination-test-job-{i}");
        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: job_id.clone(),
            destination: DestinationSpec {
                bucket: dest_bucket_str.clone(),
                prefix: Some(format!("job-{i}/")),
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

        let resp: PutInventoryConfigResponse = ctx
            .client
            .put_inventory_config(&bucket, &job_id, job)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket().unwrap().as_str(), bucket_name.as_str());
    }

    // List all jobs
    let mut all_jobs = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let list: ListInventoryConfigsResponse = if let Some(token) = continuation_token.clone() {
            ctx.client
                .list_inventory_configs(&bucket)
                .unwrap()
                .continuation_token(token)
                .build()
                .send()
                .await
                .unwrap()
        } else {
            ctx.client
                .list_inventory_configs(&bucket)
                .unwrap()
                .build()
                .send()
                .await
                .unwrap()
        };
        let configs = list.configs().unwrap();

        all_jobs.extend(configs.items.iter().map(|item| item.id.clone()));

        if !configs.has_more() {
            break;
        }

        continuation_token = configs.next_continuation_token.clone();
    }

    // Verify all jobs are in the list
    for i in 0..job_count {
        let job_id = format!("pagination-test-job-{i}");
        assert!(
            all_jobs.contains(&job_id),
            "Job {job_id} should be in the list"
        );
    }

    // Cleanup
    for i in 0..job_count {
        let job_id = format!("pagination-test-job-{i}");
        ctx.client
            .delete_inventory_config(&bucket, &job_id)
            .unwrap()
            .build()
            .send()
            .await
            .ok();
    }

    ctx.client
        .delete_bucket(dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .ok();
}
