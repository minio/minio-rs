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

//! Single-run inventory scan benchmark.
//!
//! Measures scan throughput for a fixed number of objects. Designed for
//! comparing inventory performance across different MinIO server versions.
//!
//! # Usage
//!
//! ```bash
//! # Default: 100,000 objects
//! cargo run --example inventory_benchmark_scan
//!
//! # Custom object count
//! cargo run --example inventory_benchmark_scan -- --objects 50000
//!
//! # With version label (for output file naming)
//! cargo run --example inventory_benchmark_scan -- --objects 100000 --label v1-baseline
//! ```
//!
//! # Output
//!
//! Creates `benchmark_<label>.json` with results for easy comparison.

use clap::Parser;
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use minio::s3::MinioClient;
use minio::s3inventory::{
    DestinationSpec, GenerateInventoryConfigResponse, JobDefinition, JobState, JobStatus,
    ModeSpec, OnOrOff, OutputFormat, Schedule, VersionsSpec,
};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Parser, Debug)]
#[command(about = "Single-run inventory scan benchmark for comparing server versions")]
struct Args {
    /// Number of objects to scan
    #[arg(long, default_value = "100000")]
    objects: usize,

    /// Version label for output file naming
    #[arg(long, default_value = "test")]
    label: String,

    /// Number of concurrent upload workers
    #[arg(long, default_value = "16")]
    uploaders: usize,

    /// Skip cleanup (keep objects after test)
    #[arg(long, default_value = "false")]
    skip_cleanup: bool,

    /// MinIO server endpoint
    #[arg(long, default_value = "http://localhost:9000")]
    endpoint: String,

    /// Access key
    #[arg(long, default_value = "minioadmin")]
    access_key: String,

    /// Secret key
    #[arg(long, default_value = "minioadmin")]
    secret_key: String,
}

#[derive(serde::Serialize)]
struct BenchmarkResult {
    label: String,
    object_count: usize,
    upload_duration_secs: f64,
    upload_throughput: f64,
    scan_duration_secs: f64,
    scan_throughput: f64,
    timestamp: String,
    endpoint: String,
}

async fn upload_objects(
    client: &MinioClient,
    bucket: &BucketName,
    object_count: usize,
    concurrent_uploaders: usize,
) -> (Duration, u64) {
    let upload_start = Instant::now();
    let error_count = Arc::new(AtomicU64::new(0));
    let progress = Arc::new(AtomicU64::new(0));

    let mut tasks = JoinSet::new();
    let objects_per_task = object_count / concurrent_uploaders;

    for i in 0..concurrent_uploaders {
        let start_idx = i * objects_per_task;
        let end_idx = if i == concurrent_uploaders - 1 {
            object_count
        } else {
            (i + 1) * objects_per_task
        };

        let client = client.clone();
        let bucket = bucket.clone();
        let error_count = Arc::clone(&error_count);
        let progress = Arc::clone(&progress);

        tasks.spawn(async move {
            for idx in start_idx..end_idx {
                let object_name = format!("benchmark/obj-{:08}.dat", idx);
                let content = vec![b'X'; 1024]; // 1KB objects
                let object_content = ObjectContent::from(content);

                if let Err(_e) = client
                    .put_object_content(
                        bucket.clone(),
                        ObjectKey::new(&object_name).unwrap(),
                        object_content,
                    )
                    .unwrap()
                    .build()
                    .send()
                    .await
                {
                    error_count.fetch_add(1, Ordering::Relaxed);
                }

                let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 10000 == 0 {
                    eprintln!("  Uploaded {}/{} objects...", done, object_count);
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Upload task panic: {}", e);
        }
    }

    (upload_start.elapsed(), error_count.load(Ordering::Relaxed))
}

async fn run_inventory_scan(
    client: &MinioClient,
    bucket: &BucketName,
    job_id: &str,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let scan_start = Instant::now();

    // Trigger scan
    let _: GenerateInventoryConfigResponse = client
        .generate_inventory_config(bucket.clone(), job_id)?
        .build()
        .send()
        .await?;

    // Poll for completion
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let resp = client
            .get_inventory_job_status(bucket.clone(), job_id)?
            .build()
            .send()
            .await?;

        let status: JobStatus = resp.status()?;

        match status.state {
            JobState::Completed => {
                return Ok(scan_start.elapsed());
            }
            JobState::Failed => {
                return Err("Inventory scan failed".into());
            }
            JobState::Running => {
                let elapsed = scan_start.elapsed().as_secs();
                if elapsed % 10 == 0 && elapsed > 0 {
                    eprint!("\r  Scanning... {}s elapsed", elapsed);
                }
            }
            _ => {}
        }

        // Timeout after 30 minutes
        if scan_start.elapsed() > Duration::from_secs(1800) {
            return Err("Inventory scan timeout (30 min)".into());
        }
    }
}

async fn delete_objects(
    client: &MinioClient,
    bucket: &BucketName,
    object_count: usize,
    concurrent_deleters: usize,
) {
    let progress = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    let objects_per_task = object_count / concurrent_deleters;

    for i in 0..concurrent_deleters {
        let start_idx = i * objects_per_task;
        let end_idx = if i == concurrent_deleters - 1 {
            object_count
        } else {
            (i + 1) * objects_per_task
        };

        let client = client.clone();
        let bucket = bucket.clone();
        let progress = Arc::clone(&progress);

        tasks.spawn(async move {
            for idx in start_idx..end_idx {
                let object_name = format!("benchmark/obj-{:08}.dat", idx);
                let _ = client
                    .delete_object(bucket.clone(), &object_name)
                    .unwrap()
                    .build()
                    .send()
                    .await;

                let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 10000 == 0 {
                    eprintln!("  Deleted {}/{} objects...", done, object_count);
                }
            }
        });
    }

    while let Some(_) = tasks.join_next().await {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    println!("========================================");
    println!("  INVENTORY SCAN BENCHMARK");
    println!("========================================");
    println!();
    println!("Configuration:");
    println!("  Label:      {}", args.label);
    println!("  Objects:    {}", args.objects);
    println!("  Uploaders:  {}", args.uploaders);
    println!("  Endpoint:   {}", args.endpoint);
    println!();

    let base_url = args.endpoint.parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new(&args.access_key, &args.secret_key, None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None)?;

    let bucket = BucketName::new("scan-benchmark").unwrap();
    let dest_bucket = BucketName::new("scan-benchmark-reports").unwrap();
    let job_id = "benchmark-scan";

    // Setup
    println!("[1/5] Creating buckets...");
    let _ = client.create_bucket(bucket.clone()).unwrap().build().send().await;
    let _ = client.create_bucket(dest_bucket.clone()).unwrap().build().send().await;

    // Create inventory config
    println!("[2/5] Creating inventory configuration...");
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.to_string(),
            prefix: Some("benchmark/".to_string()),
            format: OutputFormat::Parquet,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Once,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    client
        .put_inventory_config(bucket.clone(), job_id, job)?
        .build()
        .send()
        .await?;

    // Upload objects
    println!("[3/5] Uploading {} objects...", args.objects);
    let (upload_duration, upload_errors) =
        upload_objects(&client, &bucket, args.objects, args.uploaders).await;
    let upload_throughput = args.objects as f64 / upload_duration.as_secs_f64();
    println!(
        "      Done in {:.1}s ({:.0} obj/s, {} errors)",
        upload_duration.as_secs_f64(),
        upload_throughput,
        upload_errors
    );

    // Run scan
    println!("[4/5] Running inventory scan...");
    let scan_duration = run_inventory_scan(&client, &bucket, job_id).await?;
    let scan_throughput = args.objects as f64 / scan_duration.as_secs_f64();
    println!();
    println!(
        "      Done in {:.1}s ({:.0} obj/s)",
        scan_duration.as_secs_f64(),
        scan_throughput
    );

    // Cleanup
    if !args.skip_cleanup {
        println!("[5/5] Cleaning up...");
        delete_objects(&client, &bucket, args.objects, args.uploaders).await;
        println!("      Done");
    } else {
        println!("[5/5] Skipping cleanup (--skip-cleanup)");
    }

    // Results
    println!();
    println!("========================================");
    println!("  RESULTS");
    println!("========================================");
    println!();
    println!("  Label:            {}", args.label);
    println!("  Objects:          {}", args.objects);
    println!("  Scan Duration:    {:.2}s", scan_duration.as_secs_f64());
    println!("  Scan Throughput:  {:.0} objects/sec", scan_throughput);
    println!();

    // Save result to JSON
    let result = BenchmarkResult {
        label: args.label.clone(),
        object_count: args.objects,
        upload_duration_secs: upload_duration.as_secs_f64(),
        upload_throughput,
        scan_duration_secs: scan_duration.as_secs_f64(),
        scan_throughput,
        timestamp: chrono::Utc::now().to_rfc3339(),
        endpoint: args.endpoint,
    };

    let json_file = format!("benchmark_{}.json", args.label);
    let mut file = File::create(&json_file)?;
    writeln!(file, "{}", serde_json::to_string_pretty(&result)?)?;
    println!("Results saved to: {}", json_file);

    // Also append to CSV for easy comparison
    let csv_file = "benchmark_results.csv";
    let write_header = !std::path::Path::new(csv_file).exists();
    let mut csv = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_file)?;

    if write_header {
        writeln!(csv, "label,objects,scan_duration_secs,scan_throughput,timestamp")?;
    }
    writeln!(
        csv,
        "{},{},{:.2},{:.0},{}",
        args.label, args.objects, scan_duration.as_secs_f64(), scan_throughput, result.timestamp
    )?;
    println!("Results appended to: {}", csv_file);

    println!();
    println!("========================================");

    Ok(())
}
