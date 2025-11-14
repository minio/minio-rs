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

//! Inventory job monitoring and control example.
//!
//! This example demonstrates the difference between two inventory systems:
//!
//! # Two Inventory Systems
//!
//! **S3 Inventory API** (AWS-compatible, portable):
//! - Listed via `list_inventory_configs()` and `get_inventory_config()`
//! - Creates inventory report documents (CSV, Parquet, etc.)
//! - Works on AWS S3 and S3-compatible services
//! - Read-only configuration on MinIO
//!
//! **Admin Inventory Control** (MinIO-specific):
//! - Job lifecycle management: suspend, resume, cancel
//! - Only available on MinIO servers
//! - Requires admin credentials
//! - Controls when jobs run and whether they're active
//!
//! # Example Workflow
//!
//! This example:
//! 1. Gets the status of an inventory job using the S3 Inventory API
//! 2. Suspends the job using admin controls
//! 3. Waits 2 seconds
//! 4. Resumes the job using admin controls
//!
//! # Requirements
//!
//! - MinIO server running at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin
//! - Bucket "my-bucket" with an inventory job "daily-inventory"

use minio::madmin::MinioAdminClient;
use minio::madmin::types::MadminApi;
use minio::madmin::{AdminControlJson, AdminInventoryControlResponse};
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, S3Api};
use minio::s3inventory::{GetInventoryJobStatusResponse, JobStatus};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None)?;

    let bucket = BucketName::new("my-bucket").unwrap();
    let job_id = "daily-inventory";

    println!("Monitoring inventory job '{job_id}'...\n");

    // Get job status using S3 Inventory API (AWS-compatible)
    let resp: GetInventoryJobStatusResponse = client
        .get_inventory_job_status(bucket.clone(), job_id)?
        .build()
        .send()
        .await?;
    let job_status: JobStatus = resp.status()?;
    println!("S3 Inventory API - Job Status: {job_status}");

    println!("\nMinIO Admin Operations:");

    // Create admin client from regular client
    let admin: MinioAdminClient = client.admin();

    // Suspend the job (pause it without canceling)
    println!("  1. Suspending job...");
    let resp: AdminInventoryControlResponse = admin
        .suspend_inventory_job(&bucket, job_id)?
        .build()
        .send()
        .await?;
    let admin_control: AdminControlJson = resp.admin_control()?;
    println!("     Result: {}", admin_control.status);
    println!("     Job ID: {}", admin_control.inventory_id);
    println!("     Bucket: {}", admin_control.bucket);

    // Wait to demonstrate pause
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Resume the job (re-enable scheduling)
    println!("  2. Resuming job (after pause)...");
    let resp: AdminInventoryControlResponse = admin
        .resume_inventory_job(&bucket, job_id)?
        .build()
        .send()
        .await?;
    let admin_control: AdminControlJson = resp.admin_control()?;
    println!("     Result: {}", admin_control.status);

    println!("\nNote: Use cancel_inventory_job() to permanently cancel a job.");

    Ok(())
}
