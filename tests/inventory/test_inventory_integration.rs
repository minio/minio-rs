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
    DestinationSpec, FilterSpec, JobDefinition, ModeSpec, NameFilter, OnOrOff, OutputFormat,
    Schedule, SizeFilter, VersionsSpec,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use std::time::Duration;

#[minio_macros::test(no_cleanup)]
async fn inventory_complete_workflow(ctx: TestContext, bucket_name: String) {
    let job_id = "integration-test-job";
    let dest_bucket = format!("{bucket_name}-reports");

    // Step 1: Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Step 2: Generate template (optional but shows API usage)
    let template = ctx
        .client
        .generate_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert!(
        !template.yaml_template().is_empty(),
        "Template should be generated"
    );

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

    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
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

    ctx.client
        .put_inventory_config(&bucket_name, job_id, job)
        .build()
        .send()
        .await
        .unwrap();

    // Step 4: Verify job was created by listing
    let list = ctx
        .client
        .list_inventory_configs(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();

    let found = list.items().iter().any(|item| item.id == job_id);
    assert!(found, "Job should appear in list");

    // Step 5: Get job configuration
    let config = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(config.id(), job_id);
    assert!(!config.yaml_definition().is_empty());

    // Step 6: Get job status
    let status = ctx
        .client
        .get_inventory_job_status(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(status.id(), job_id);
    assert_eq!(status.bucket(), bucket_name);

    // Step 7: Test admin operations
    let admin = ctx.client.admin();

    // Suspend
    let suspend_resp = admin
        .suspend_inventory_job(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(suspend_resp.status(), "suspended");

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Resume
    let resume_resp = admin
        .resume_inventory_job(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resume_resp.status(), "resumed");

    // Step 8: Update job configuration (via put with same ID)
    let updated_job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
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

    ctx.client
        .put_inventory_config(&bucket_name, job_id, updated_job)
        .build()
        .send()
        .await
        .unwrap();

    // Verify update
    let updated_config = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    let yaml = updated_config.yaml_definition();
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
    ctx.client
        .delete_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    // Verify deletion
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

#[minio_macros::test(no_cleanup)]
async fn inventory_pagination_test(ctx: TestContext, bucket_name: String) {
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Create multiple jobs to test pagination
    let job_count = 5;
    for i in 0..job_count {
        let job_id = format!("pagination-test-job-{i}");
        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: job_id.clone(),
            destination: DestinationSpec {
                bucket: dest_bucket.clone(),
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

        ctx.client
            .put_inventory_config(&bucket_name, &job_id, job)
            .build()
            .send()
            .await
            .unwrap();
    }

    // List all jobs
    let mut all_jobs = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let list = if let Some(token) = continuation_token.clone() {
            ctx.client
                .list_inventory_configs(&bucket_name)
                .continuation_token(token)
                .build()
                .send()
                .await
                .unwrap()
        } else {
            ctx.client
                .list_inventory_configs(&bucket_name)
                .build()
                .send()
                .await
                .unwrap()
        };

        all_jobs.extend(list.items().iter().map(|item| item.id.clone()));

        if !list.has_more() {
            break;
        }

        continuation_token = list.next_continuation_token().map(String::from);
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
            .delete_inventory_config(&bucket_name, &job_id)
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
