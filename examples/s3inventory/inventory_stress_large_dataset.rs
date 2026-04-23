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

//! Saturation test: Inventory scan performance with increasing dataset sizes.
//!
//! Tests how inventory scan performance scales with dataset size by running
//! scans on buckets with incrementally increasing object counts.

use clap::Parser;
use minio::madmin::MinioAdminClient;
use minio::s3::MinioClient;
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use minio::s3inventory::{
    DestinationSpec, GenerateInventoryConfigResponse, JobDefinition, JobState, JobStatus, ModeSpec,
    OnOrOff, OutputFormat, Schedule, VersionsSpec,
};
use std::fs::File;
use std::io::Write as IoWrite;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Parser, Debug)]
#[command(about = "S3 Inventory stress test: Large dataset scan saturation analysis")]
struct Args {
    #[arg(long, default_value = "100")]
    start_objects: usize,

    #[arg(long, default_value = "5000")]
    max_objects: usize,

    #[arg(long, default_value = "500")]
    step_size: usize,

    #[arg(long, default_value = "8")]
    concurrent_uploaders: usize,
}

#[derive(Debug, Clone)]
struct ErrorDetail {
    timestamp: String,
    operation: String,
    object_count: usize,
    error_type: String,
    error_message: String,
}

struct MetricsCollector {
    upload_errors: AtomicU64,
    scan_errors: AtomicU64,
    error_details: Mutex<Vec<ErrorDetail>>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            upload_errors: AtomicU64::new(0),
            scan_errors: AtomicU64::new(0),
            error_details: Mutex::new(Vec::new()),
        }
    }

    fn record_error(&self, detail: ErrorDetail) {
        let mut errors = self.error_details.lock().unwrap();
        if errors.len() < 10 {
            errors.push(detail);
        }
    }

    fn get_error_details(&self) -> Vec<ErrorDetail> {
        self.error_details.lock().unwrap().clone()
    }
}

async fn upload_objects(
    client: &MinioClient,
    bucket: &BucketName,
    object_count: usize,
    concurrent_uploaders: usize,
    metrics: &Arc<MetricsCollector>,
) -> Duration {
    let upload_start = Instant::now();
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
        let metrics = Arc::clone(metrics);

        tasks.spawn(async move {
            for idx in start_idx..end_idx {
                let object_name = format!("data/object-{:08}.dat", idx);
                let content = vec![b'D'; 1024];
                let object_content = ObjectContent::from(content);

                if let Err(e) = client
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
                    metrics.upload_errors.fetch_add(1, Ordering::Relaxed);
                    metrics.record_error(ErrorDetail {
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        operation: "upload".to_string(),
                        object_count,
                        error_type: "UploadError".to_string(),
                        error_message: format!("{:?}", e),
                    });
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Upload task error: {}", e);
        }
    }

    upload_start.elapsed()
}

async fn run_inventory_scan(
    client: &MinioClient,
    _admin: &MinioAdminClient,
    bucket: &BucketName,
    job_id: &str,
    object_count: usize,
    metrics: &Arc<MetricsCollector>,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let scan_start = Instant::now();

    let _: GenerateInventoryConfigResponse = client
        .generate_inventory_config(bucket.clone(), job_id)?
        .build()
        .send()
        .await?;

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        match client
            .get_inventory_job_status(bucket.clone(), job_id)?
            .build()
            .send()
            .await
        {
            Ok(resp) => {
                let status: JobStatus = resp.status()?;
                match status.state {
                    JobState::Completed => {
                        return Ok(scan_start.elapsed());
                    }
                    JobState::Failed => {
                        metrics.scan_errors.fetch_add(1, Ordering::Relaxed);
                        metrics.record_error(ErrorDetail {
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            operation: "scan".to_string(),
                            object_count,
                            error_type: "ScanFailed".to_string(),
                            error_message: "Inventory job failed".to_string(),
                        });
                        return Err("Inventory scan failed".into());
                    }
                    _ => continue,
                }
            }
            Err(e) => {
                eprintln!("Status check error: {}", e);
            }
        }

        if scan_start.elapsed() > Duration::from_secs(600) {
            return Err("Inventory scan timeout".into());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    println!("=== S3 Inventory Stress Test: Large Dataset Scan Saturation ===\n");

    let levels = ((args.max_objects - args.start_objects) / args.step_size) + 1;

    println!("Configuration:");
    println!("  Start objects:        {}", args.start_objects);
    println!("  Max objects:          {}", args.max_objects);
    println!("  Step size:            {}", args.step_size);
    println!("  Concurrent uploaders: {}", args.concurrent_uploaders);
    println!("  Total levels:         {}\n", levels);

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;

    let bucket = BucketName::new("large-dataset-bucket").unwrap();
    let dest_bucket = BucketName::new("large-dataset-reports").unwrap();
    let job_id = "scan-job";

    println!("Step 1: Creating test buckets...");
    let _ = client.create_bucket(bucket.clone()).unwrap().build().send().await;
    let _ = client
        .create_bucket(dest_bucket.clone())
        .unwrap()
        .build()
        .send()
        .await;
    println!("  Buckets ready\n");

    println!("Step 2: Creating inventory configuration...");
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.to_string(),
            prefix: Some("scans/".to_string()),
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
    println!("  Inventory config created\n");

    println!("Step 3: Running saturation test...\n");

    let mut csv_file = File::create("large_dataset_saturation.csv")?;
    writeln!(
        csv_file,
        "object_count,upload_duration_secs,upload_throughput,scan_duration_secs,scan_throughput,\
         upload_errors,scan_errors"
    )?;

    let admin: MinioAdminClient = client.admin();
    let test_start = Instant::now();
    let all_errors = Arc::new(Mutex::new(Vec::new()));

    let mut current_objects = args.start_objects;
    while current_objects <= args.max_objects {
        let elapsed_secs = test_start.elapsed().as_secs_f64();

        println!(
            "[{:>6.1}s] Starting level: {} objects",
            elapsed_secs, current_objects
        );

        let metrics = Arc::new(MetricsCollector::new());

        // Upload objects
        print!("  Uploading {} objects... ", current_objects);
        std::io::stdout().flush().unwrap();
        let upload_duration = upload_objects(
            &client,
            &bucket,
            current_objects,
            args.concurrent_uploaders,
            &metrics,
        )
        .await;
        let upload_throughput = current_objects as f64 / upload_duration.as_secs_f64();
        println!(
            "{:.1}s ({:.0} ops/sec)",
            upload_duration.as_secs_f64(),
            upload_throughput
        );

        // Run inventory scan
        print!("  Running inventory scan... ");
        std::io::stdout().flush().unwrap();
        let scan_duration =
            match run_inventory_scan(&client, &admin, &bucket, job_id, current_objects, &metrics)
                .await
            {
                Ok(dur) => dur,
                Err(e) => {
                    eprintln!("Scan failed: {}", e);
                    Duration::from_secs(0)
                }
            };
        let scan_throughput = if scan_duration.as_secs() > 0 {
            current_objects as f64 / scan_duration.as_secs_f64()
        } else {
            0.0
        };
        println!(
            "{:.1}s ({:.0} ops/sec)",
            scan_duration.as_secs_f64(),
            scan_throughput
        );

        // Collect errors
        {
            let mut all_errs = all_errors.lock().unwrap();
            all_errs.extend(metrics.get_error_details());
        }

        writeln!(
            csv_file,
            "{},{:.2},{:.2},{:.2},{:.2},{},{}",
            current_objects,
            upload_duration.as_secs_f64(),
            upload_throughput,
            scan_duration.as_secs_f64(),
            scan_throughput,
            metrics.upload_errors.load(Ordering::Relaxed),
            metrics.scan_errors.load(Ordering::Relaxed)
        )?;

        println!(
            "[{:>6.1}s] Completed: Upload={:.0} ops/sec | Scan={:.0} ops/sec | Errors: {}+{}\n",
            test_start.elapsed().as_secs_f64(),
            upload_throughput,
            scan_throughput,
            metrics.upload_errors.load(Ordering::Relaxed),
            metrics.scan_errors.load(Ordering::Relaxed)
        );

        // Clean up objects for next level
        println!("  Cleaning up objects...");
        let mut delete_tasks = JoinSet::new();
        for i in 0..current_objects {
            let client = client.clone();
            let bucket = bucket.clone();
            let object_name = format!("data/object-{:08}.dat", i);

            delete_tasks.spawn(async move {
                let _ = client
                    .delete_object(bucket, &object_name)
                    .unwrap()
                    .build()
                    .send()
                    .await;
            });

            if i % 100 == 0 {
                while let Some(result) = delete_tasks.join_next().await {
                    let _ = result;
                }
            }
        }
        while let Some(result) = delete_tasks.join_next().await {
            let _ = result;
        }

        current_objects += args.step_size;
    }

    println!("\n=== Saturation Test Completed ===");
    println!(
        "Total duration: {:.1}s\n",
        test_start.elapsed().as_secs_f64()
    );

    // Write error details
    let error_details = all_errors.lock().unwrap();
    let mut error_file = File::create("large_dataset_errors.txt")?;
    writeln!(error_file, "=== LARGE DATASET ERROR ANALYSIS ===")?;
    writeln!(error_file, "Test Date: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(error_file, "Total Errors Captured: {}", error_details.len())?;
    writeln!(error_file)?;
    writeln!(error_file, "=== ERROR DETAILS (Last 10) ===")?;
    writeln!(error_file)?;

    if error_details.is_empty() {
        writeln!(error_file, "No errors detected during test.")?;
    } else {
        for (i, err) in error_details.iter().enumerate() {
            writeln!(error_file, "Error {}:", i + 1)?;
            writeln!(error_file, "  Timestamp:     {}", err.timestamp)?;
            writeln!(error_file, "  Operation:     {}", err.operation)?;
            writeln!(error_file, "  Object Count:  {}", err.object_count)?;
            writeln!(error_file, "  Error Type:    {}", err.error_type)?;
            writeln!(error_file, "  Message:       {}", err.error_message)?;
            writeln!(error_file)?;
        }
    }

    println!("[OK] Results written to: large_dataset_saturation.csv");
    println!("[OK] Error details written to: large_dataset_errors.txt");
    println!("\n[NEXT] Visualize results:");
    println!("  python examples/s3inventory/plot_large_dataset.py");

    Ok(())
}
