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

//! Integration tests for Batch Operations APIs
//!
//! Tests the following APIs:
//! - StartBatchJob (start batch job)
//! - BatchJobStatus (get job status)
//! - DescribeBatchJob (describe job)
//! - GenerateBatchJob (generate template)
//! - GetSupportedBatchJobTypes (list supported types)
//! - GenerateBatchJobV2 (generate template from server)
//! - ListBatchJobs (list all jobs)
//! - CancelBatchJob (cancel job)

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{
    BatchJobStatusResponse, CancelBatchJobResponse, DescribeBatchJobResponse,
    GenerateBatchJobResponse, GenerateBatchJobV2Response, GetSupportedBatchJobTypesResponse,
    ListBatchJobsResponse, StartBatchJobResponse,
};
use minio::madmin::types::batch::{BatchJobType, GenerateBatchJobOpts, ListBatchJobsFilter};
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

#[tokio::test]
async fn test_generate_batch_job_template() {
    let madmin = get_madmin_client();

    // Test generating replicate job template
    println!("Generating replicate job template...");
    let opts = GenerateBatchJobOpts::new(BatchJobType::Replicate);
    let template: GenerateBatchJobResponse = madmin.generate_batch_job(opts);

    println!("Replicate template (truncated):");
    println!("{}", &template.template[..200.min(template.template.len())]);

    assert!(template.template.contains("replicate:"));
    assert!(template.template.contains("apiVersion:"));
    assert!(template.template.contains("source:"));
    assert!(template.template.contains("target:"));

    // Test generating key rotation job template
    println!("\nGenerating key rotation job template...");
    let opts = GenerateBatchJobOpts::new(BatchJobType::KeyRotate);
    let template: GenerateBatchJobResponse = madmin.generate_batch_job(opts);

    println!("KeyRotate template (truncated):");
    println!("{}", &template.template[..200.min(template.template.len())]);

    assert!(template.template.contains("keyrotate:"));
    assert!(template.template.contains("encryption:"));

    // Test generating expiry job template
    println!("\nGenerating expiry job template...");
    let opts = GenerateBatchJobOpts::new(BatchJobType::Expire);
    let template: GenerateBatchJobResponse = madmin.generate_batch_job(opts);

    println!("Expire template (truncated):");
    println!("{}", &template.template[..200.min(template.template.len())]);

    assert!(template.template.contains("expire:"));
    assert!(template.template.contains("rules:"));
}

#[tokio::test]
#[ignore = "list-supported-job-types API is not supported in MinIO mode-server-xl (standard deployment mode)"]
async fn test_get_supported_batch_job_types() {
    let madmin = get_madmin_client();

    let result: GetSupportedBatchJobTypesResponse = madmin
        .get_supported_batch_job_types()
        .await
        .expect("Failed to get supported job types");

    println!("Supported batch job types:");
    println!("  API Available: {}", !result.api_unavailable);

    if !result.api_unavailable {
        println!("  Types: {} type(s)", result.supported_types.len());
        for job_type in &result.supported_types {
            println!("    - {:?}", job_type);
        }

        // MinIO should support at least replicate, keyrotate, and expire
        assert!(
            !result.supported_types.is_empty(),
            "Should have at least one supported type"
        );
    } else {
        println!("  Note: Batch job API not available on this server version");
    }
}

#[tokio::test]
async fn test_list_batch_jobs() {
    let madmin = get_madmin_client();

    // List all jobs
    println!("Listing all batch jobs...");
    let response: ListBatchJobsResponse = madmin
        .list_batch_jobs(None)
        .await
        .expect("Failed to list batch jobs");

    let result = response.jobs().expect("Failed to parse jobs");
    println!("Found {} batch job(s)", result.jobs.len());

    for job in &result.jobs {
        println!("Job ID: {}", job.id);
        println!("  Type: {:?}", job.job_type);

        if let Some(ref bucket) = job.bucket {
            println!("  Bucket: {}", bucket);
        }

        if let Some(ref user) = job.user {
            println!("  User: {}", user);
        }

        println!("  Started: {}", job.started);

        if let Some(ref status) = job.status {
            println!("  Status: {:?}", status);
        }

        if let Some(ref error) = job.error {
            println!("  Error: {}", error);
        }
    }
}

#[tokio::test]
async fn test_list_batch_jobs_with_filter() {
    let madmin = get_madmin_client();

    // List jobs filtered by type
    println!("Listing replicate batch jobs...");
    let filter = ListBatchJobsFilter::new().with_job_type("replicate".to_string());

    let response: ListBatchJobsResponse = madmin
        .list_batch_jobs(Some(filter))
        .await
        .expect("Failed to list filtered batch jobs");

    let result = response.jobs().expect("Failed to parse jobs");
    println!("Found {} replicate job(s)", result.jobs.len());

    for job in &result.jobs {
        assert_eq!(job.job_type, BatchJobType::Replicate);
        println!("  Job: {} (started: {})", job.id, job.started);
    }
}

#[tokio::test]
#[ignore]
async fn test_start_and_cancel_batch_job() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Requires valid replication targets or storage configuration
    // 2. Creates actual batch jobs that run on the cluster
    // 3. May transfer data and affect performance
    //
    // To run this test:
    // - Configure replication targets or appropriate setup
    // - Remove the skip attribute
    // - Run: cargo test test_start_and_cancel_batch_job -- --nocapture

    // Generate a simple replicate job YAML
    let opts = GenerateBatchJobOpts::new(BatchJobType::Replicate);
    let template: GenerateBatchJobResponse = madmin.generate_batch_job(opts);

    // Modify the template with actual values (simplified for test)
    let job_yaml = template
        .template
        .replace("TYPE", "minio")
        .replace("BUCKET", "test-source-bucket")
        .replace("PREFIX", "data/")
        .replace("http[s]://HOSTNAME:PORT", "http://localhost:9000")
        .replace("ACCESS-KEY", "minioadmin")
        .replace("SECRET-KEY", "minioadmin");

    println!("Starting batch job...");
    println!("Job YAML (truncated):");
    println!("{}", &job_yaml[..300.min(job_yaml.len())]);

    // Start the job
    let response: StartBatchJobResponse = madmin
        .start_batch_job(&job_yaml)
        .await
        .expect("Failed to start batch job");

    let result = response.result().expect("Failed to parse result");
    println!("Job started:");
    println!("  ID: {}", result.id);
    println!("  Type: {:?}", result.job_type);

    if let Some(ref bucket) = result.bucket {
        println!("  Bucket: {}", bucket);
    }

    // Get job status
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let status_response: BatchJobStatusResponse = madmin
        .batch_job_status(&result.id)
        .await
        .expect("Failed to get job status");

    let last_metric = status_response
        .last_metric()
        .expect("Failed to parse status");
    println!("Job status retrieved: {:?}", last_metric);

    // Describe the job
    let description: DescribeBatchJobResponse = madmin
        .describe_batch_job(&result.id)
        .await
        .expect("Failed to describe job");

    println!("Job description (truncated):");
    println!(
        "{}",
        &description.job_yaml[..200.min(description.job_yaml.len())]
    );

    // Cancel the job
    println!("Canceling job...");
    let _cancel: CancelBatchJobResponse = madmin
        .cancel_batch_job(&result.id)
        .await
        .expect("Failed to cancel job");

    println!("Job canceled successfully");
}

#[tokio::test]
#[ignore]
async fn test_generate_batch_job_v2() {
    let madmin = get_madmin_client();
    // NOTE: This test may not work on all MinIO versions
    // GenerateBatchJobV2 is an EOS-only API

    let opts = GenerateBatchJobOpts::new(BatchJobType::Replicate);

    let result: GenerateBatchJobV2Response = madmin
        .generate_batch_job_v2(opts)
        .await
        .expect("Failed to generate batch job v2");

    if result.api_unavailable {
        println!("GenerateBatchJobV2 API not available on this server");
    } else {
        println!("Generated template from server (truncated):");
        println!("{}", &result.template[..200.min(result.template.len())]);

        assert!(!result.template.is_empty());
    }
}

#[tokio::test]
async fn test_batch_job_type_serialization() {
    let _madmin = get_madmin_client();
    use serde_json;

    // Test BatchJobType serialization
    let job_type = BatchJobType::Replicate;
    let json = serde_json::to_string(&job_type).unwrap();
    assert_eq!(json, "\"replicate\"");

    let job_type = BatchJobType::KeyRotate;
    let json = serde_json::to_string(&job_type).unwrap();
    assert_eq!(json, "\"keyrotate\"");

    let job_type = BatchJobType::Expire;
    let json = serde_json::to_string(&job_type).unwrap();
    assert_eq!(json, "\"expire\"");

    let job_type = BatchJobType::Catalog;
    let json = serde_json::to_string(&job_type).unwrap();
    assert_eq!(json, "\"catalog\"");
}

#[tokio::test]
async fn test_list_batch_jobs_filter_builder() {
    let _madmin = get_madmin_client();
    let filter = ListBatchJobsFilter::new()
        .with_job_type("replicate".to_string())
        .with_bucket("my-bucket".to_string());

    assert_eq!(filter.by_job_type, Some("replicate".to_string()));
    assert_eq!(filter.by_bucket, Some("my-bucket".to_string()));
}
