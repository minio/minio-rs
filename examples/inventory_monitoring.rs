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
//! This example demonstrates monitoring job status and using admin controls.

use minio::admin::types::AdminApi;
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None)?;

    let bucket = "my-bucket";
    let job_id = "daily-inventory";

    println!("Monitoring inventory job '{job_id}'...\n");

    let status = client
        .get_inventory_job_status(bucket, job_id)
        .build()
        .send()
        .await?;

    println!("Job Status:");
    println!("  State: {:?}", status.state());
    println!("  Scanned: {} objects", status.scanned_count());
    println!("  Matched: {} objects", status.matched_count());
    println!("  Output Files: {}", status.output_files_count());

    if let Some(manifest) = status.status().manifest_path.as_ref() {
        println!("  Manifest: {manifest}");
    }

    if let Some(start) = status.status().start_time {
        println!("  Started: {start}");
    }

    if let Some(end) = status.status().end_time {
        println!("  Completed: {end}");
    }

    println!("\nAdmin Operations:");

    let admin = client.admin();

    println!("  Suspending job...");
    let resp = admin
        .suspend_inventory_job(bucket, job_id)
        .build()
        .send()
        .await?;
    println!("    Status: {}", resp.status());

    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("  Resuming job...");
    let resp = admin
        .resume_inventory_job(bucket, job_id)
        .build()
        .send()
        .await?;
    println!("    Status: {}", resp.status());

    Ok(())
}
