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

//! Edge case tests for inventory operations.
//!
//! These tests verify SDK behavior in edge cases like empty buckets,
//! zero-match filters, and early job termination.

use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, S3Api};
use minio::s3::MinioClient;
use minio::s3inventory::{
    DestinationSpec, FilterSpec, JobDefinition, JobState, ModeSpec, OnOrOff, OutputFormat,
    Schedule, VersionsSpec,
};
use std::time::Duration;

async fn setup_client() -> MinioClient {
    let base_url = "http://localhost:9000".parse::<BaseUrl>().unwrap();
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    MinioClient::new(base_url, Some(static_provider), None, None).unwrap()
}

async fn wait_for_completion(
    client: &MinioClient,
    bucket: BucketName,
    job_id: &str,
    timeout_secs: u64,
) -> Result<JobState, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();

    loop {
        if start.elapsed().as_secs() > timeout_secs {
            return Err("Timeout waiting for job completion".into());
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let status = client
            .get_inventory_job_status(&bucket, job_id)
            .unwrap()
            .build()
            .send()
            .await?;

        let job_status = status.status()?;

        match job_status.state {
            JobState::Completed => return Ok(JobState::Completed),
            JobState::Failed => return Err("Job failed".into()),
            _ => continue,
        }
    }
}

#[tokio::test]
#[ignore] // Requires MinIO server running on localhost:9000
async fn test_empty_bucket_scan() {
    // This test would have caught the bug!
    // Empty bucket -> no output files -> ResultFiles is nil -> "files": null

    let client = setup_client().await;
    let bucket_str = "empty-bucket-test";
    let dest_bucket_str = "empty-reports";
    let bucket = BucketName::new(bucket_str).unwrap();
    let dest_bucket = BucketName::new(dest_bucket_str).unwrap();
    let job_id = "empty-scan-job";

    // Create empty bucket
    let _ = client.create_bucket(&bucket).build().send().await;
    let _ = client.create_bucket(&dest_bucket).build().send().await;

    // Create inventory config
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.to_string(),
            prefix: Some("scans/".to_string()),
            format: OutputFormat::JSON,
            compression: OnOrOff::Off,
            max_file_size_hint: None,
        },
        schedule: Schedule::Once,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    client
        .put_inventory_config(&bucket, job_id, job.clone())
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create inventory config");

    // Generate inventory on EMPTY bucket
    client
        .generate_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to generate inventory");

    // Wait for completion - SDK must be able to parse the response
    // This would FAIL with parse error before the server fix
    let state = wait_for_completion(&client, &bucket, job_id, 60)
        .await
        .expect("Failed to complete empty bucket scan");

    assert_eq!(state, JobState::Completed);

    // Verify we can get status without parse errors
    let status = client
        .get_inventory_job_status(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to get status after empty scan");

    let job_status = status.status().expect("Failed to parse status");
    assert_eq!(job_status.state, JobState::Completed);

    // Clean up
    let _ = client.delete_bucket(bucket).build().send().await;
    let _ = client.delete_bucket(dest_bucket).build().send().await;
}

#[tokio::test]
#[ignore] // Requires MinIO server running on localhost:9000
async fn test_zero_match_filter_scan() {
    // Bucket has objects but filters match nothing
    // Should complete successfully with empty files array

    let client = setup_client().await;
    let bucket_str = "zero-match-test";
    let dest_bucket_str = "zero-match-reports";
    let bucket = BucketName::new(bucket_str).unwrap();
    let dest_bucket = BucketName::new(dest_bucket_str).unwrap();
    let job_id = "zero-match-job";

    // Create buckets
    let _ = client.create_bucket(&bucket).build().send().await;
    let _ = client.create_bucket(&dest_bucket).build().send().await;

    // Upload some objects with specific prefix
    for i in 0..5 {
        let object_name = format!("data/file-{}.txt", i);
        let content = vec![b'X'; 100];
        let _ = client
            .put_object_content(&bucket, &object_name, ObjectContent::from(content))
            .build()
            .send()
            .await;
    }

    // Create inventory with filter that matches NOTHING
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.to_string(),
            prefix: Some("scans/".to_string()),
            format: OutputFormat::JSON,
            compression: OnOrOff::Off,
            max_file_size_hint: None,
        },
        schedule: Schedule::Once,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: Some(FilterSpec {
            prefix: Some(vec!["nonexistent/".to_string()]), // Won't match anything
            last_modified: None,
            size: None,
            versions_count: None,
            name: None,
            tags: None,
            user_metadata: None,
        }),
    };

    client
        .put_inventory_config(&bucket, job_id, job.clone())
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create inventory config");

    // Generate inventory - should find no matches
    client
        .generate_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to generate inventory");

    // Should complete successfully even with zero matches
    let state = wait_for_completion(&client, &bucket, job_id, 60)
        .await
        .expect("Failed to complete zero-match scan");

    assert_eq!(state, JobState::Completed);

    // Clean up
    for i in 0..5 {
        let _ = client
            .delete_object(&bucket, &format!("data/file-{}.txt", i))
            .build()
            .send()
            .await;
    }
    let _ = client.delete_bucket(bucket).build().send().await;
    let _ = client.delete_bucket(dest_bucket).build().send().await;
}

#[tokio::test]
#[ignore] // Requires MinIO server running on localhost:9000
async fn test_list_configs_on_empty_bucket() {
    // Tests the system.go:750 bug scenario
    // Bucket exists but has no inventory configs
    // Server was returning "items": null instead of "items": []

    let client = setup_client().await;
    let bucket = BucketName::new("list-empty-test").unwrap();

    // Create bucket
    let _ = client.create_bucket(&bucket).build().send().await;

    // List configs on bucket with NO configs
    // This would FAIL with parse error before the server fix
    let response = client
        .list_inventory_configs(&bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to list configs on empty bucket");

    let configs = response.configs().expect("Failed to parse empty configs");

    // Should have zero items, not parse error
    assert_eq!(configs.items.len(), 0);
    assert!(!configs.has_more());

    // Clean up
    let _ = client.delete_bucket(bucket).build().send().await;
}
