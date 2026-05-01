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

//! Saturation test: Write throughput while inventory scans are running.
//!
//! Tests read/write isolation by running inventory scans while concurrent
//! writers continuously add objects. Measures impact on write throughput
//! and inventory completion time.

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
#[command(about = "S3 Inventory stress test: Write-during-scan saturation analysis")]
struct Args {
    #[arg(long, default_value = "5")]
    start_writers: usize,

    #[arg(long, default_value = "25")]
    max_writers: usize,

    #[arg(long, default_value = "5")]
    step_size: usize,

    #[arg(long, default_value = "30")]
    level_duration: u64,

    #[arg(long, default_value = "100")]
    initial_objects: usize,
}

#[derive(Debug, Clone)]
struct ErrorDetail {
    timestamp: String,
    operation: String,
    writer_id: usize,
    object_name: String,
    error_type: String,
    error_message: String,
    latency_ms: u64,
}

struct MetricsCollector {
    operations: AtomicU64,
    successes: AtomicU64,
    errors: AtomicU64,
    total_latency_ms: AtomicU64,
    latencies_ms: Mutex<Vec<u64>>,
    error_details: Mutex<Vec<ErrorDetail>>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            operations: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            latencies_ms: Mutex::new(Vec::new()),
            error_details: Mutex::new(Vec::new()),
        }
    }

    fn record_success(&self, latency_ms: u64) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.latencies_ms.lock().unwrap().push(latency_ms);
    }

    fn record_error(&self, detail: ErrorDetail, latency_ms: u64) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.latencies_ms.lock().unwrap().push(latency_ms);

        let mut errors = self.error_details.lock().unwrap();
        if errors.len() < 10 {
            errors.push(detail);
        }
    }

    fn get_stats(&self) -> LevelStats {
        let ops = self.operations.load(Ordering::Relaxed);
        let successes = self.successes.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);

        let mut latencies = self.latencies_ms.lock().unwrap();
        latencies.sort_unstable();

        let p50 = if !latencies.is_empty() {
            latencies[latencies.len() / 2]
        } else {
            0
        };
        let p95 = if !latencies.is_empty() {
            latencies[latencies.len() * 95 / 100]
        } else {
            0
        };
        let p99 = if !latencies.is_empty() {
            latencies[latencies.len() * 99 / 100]
        } else {
            0
        };

        let mean = if ops > 0 {
            total_latency as f64 / ops as f64
        } else {
            0.0
        };

        LevelStats {
            total_ops: ops,
            successes,
            errors,
            latency_mean_ms: mean,
            latency_p50_ms: p50,
            latency_p95_ms: p95,
            latency_p99_ms: p99,
        }
    }

    fn get_error_details(&self) -> Vec<ErrorDetail> {
        self.error_details.lock().unwrap().clone()
    }
}

struct LevelStats {
    total_ops: u64,
    successes: u64,
    errors: u64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
}

async fn writer_task(
    client: MinioClient,
    bucket: BucketName,
    writer_id: usize,
    duration: Duration,
    metrics: Arc<MetricsCollector>,
) {
    let start = Instant::now();
    let mut object_counter = 0;

    while start.elapsed() < duration {
        let object_name = format!(
            "stress-test/writer-{}/object-{:05}.dat",
            writer_id, object_counter
        );
        let op_start = Instant::now();

        let content = vec![b'A'; 1024];
        let object_content = ObjectContent::from(content);

        match client
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
            Ok(_) => {
                let latency = op_start.elapsed().as_millis() as u64;
                metrics.record_success(latency);
            }
            Err(e) => {
                let latency = op_start.elapsed().as_millis() as u64;
                let err_msg = format!("{:?}", e);
                metrics.record_error(
                    ErrorDetail {
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        operation: "put_object".to_string(),
                        writer_id,
                        object_name: object_name.clone(),
                        error_type: "PutObjectError".to_string(),
                        error_message: err_msg,
                        latency_ms: latency,
                    },
                    latency,
                );
            }
        }

        object_counter += 1;
    }
}

async fn _run_inventory_scan(
    client: &MinioClient,
    _admin: &MinioAdminClient,
    bucket: &BucketName,
    job_id: &str,
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
                        return Err("Inventory scan failed".into());
                    }
                    _ => continue,
                }
            }
            Err(e) => {
                eprintln!("Status check error: {}", e);
            }
        }

        if scan_start.elapsed() > Duration::from_secs(300) {
            return Err("Inventory scan timeout".into());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    println!("=== S3 Inventory Stress Test: Write-During-Scan Saturation Analysis ===\n");

    let levels = ((args.max_writers - args.start_writers) / args.step_size) + 1;
    let total_time = levels as u64 * args.level_duration;

    println!("Configuration:");
    println!("  Start writers:    {}", args.start_writers);
    println!("  Max writers:      {}", args.max_writers);
    println!("  Step size:        {}", args.step_size);
    println!("  Level duration:   {}s", args.level_duration);
    println!("  Initial objects:  {}", args.initial_objects);
    println!("  Total levels:     {}", levels);
    println!(
        "  Estimated time:   ~{}s ({:.1} min)\n",
        total_time,
        total_time as f64 / 60.0
    );

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;

    let source_bucket = BucketName::new("stress-scan-bucket").unwrap();
    let dest_bucket = BucketName::new("stress-scan-reports").unwrap();
    let job_id = "scan-job";

    println!("Step 1: Creating test buckets...");
    let _ = client
        .create_bucket(source_bucket.clone())
        .unwrap()
        .build()
        .send()
        .await;
    let _ = client
        .create_bucket(dest_bucket.clone())
        .unwrap()
        .build()
        .send()
        .await;
    println!("  Buckets ready\n");

    println!(
        "Step 2: Pre-populating bucket with {} objects...",
        args.initial_objects
    );
    for i in 0..args.initial_objects {
        let object_name = format!("initial/object-{:05}.dat", i);
        let content = vec![b'A'; 1024];
        let object_content = ObjectContent::from(content);
        client
            .put_object_content(
                source_bucket.clone(),
                ObjectKey::new(&object_name).unwrap(),
                object_content,
            )
            .unwrap()
            .build()
            .send()
            .await?;
    }
    println!("  Initial objects created\n");

    println!("Step 3: Creating inventory job...");
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.to_string(),
            prefix: Some("reports/".to_string()),
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

    client
        .put_inventory_config(source_bucket.clone(), job_id, job)?
        .build()
        .send()
        .await?;
    println!("  Inventory job created\n");

    println!("Step 4: Running saturation test...\n");

    let mut csv_file = File::create("write_during_scan_saturation.csv")?;
    writeln!(
        csv_file,
        "concurrent_writers,elapsed_secs,write_ops,write_throughput,scan_duration_secs,\
         latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,successes,errors"
    )?;

    let _admin: MinioAdminClient = client.admin();
    let test_start = Instant::now();
    let all_errors = Arc::new(Mutex::new(Vec::new()));

    let mut current_writers = args.start_writers;
    while current_writers <= args.max_writers {
        let level_start = Instant::now();
        let elapsed_secs = test_start.elapsed().as_secs_f64();

        println!(
            "[{:>6.1}s] Starting load level: {} concurrent writers",
            elapsed_secs, current_writers
        );

        let metrics = Arc::new(MetricsCollector::new());

        let mut tasks = JoinSet::new();
        for writer_id in 0..current_writers {
            let client_clone =
                MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;
            let bucket = source_bucket.clone();
            let metrics_clone = Arc::clone(&metrics);
            let duration = Duration::from_secs(args.level_duration);

            tasks.spawn(async move {
                writer_task(client_clone, bucket, writer_id, duration, metrics_clone).await;
            });
        }

        // Trigger inventory scan shortly after writers start
        tokio::time::sleep(Duration::from_millis(500)).await;
        let scan_start = Instant::now();
        match client
            .generate_inventory_config(source_bucket.clone(), job_id)?
            .build()
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to trigger inventory scan: {}", e);
            }
        }

        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                eprintln!("Writer task error: {}", e);
            }
        }

        let level_duration = level_start.elapsed().as_secs_f64();
        let scan_duration = scan_start.elapsed().as_secs_f64();
        let stats = metrics.get_stats();

        let throughput = stats.total_ops as f64 / level_duration;
        let error_rate = if stats.total_ops > 0 {
            stats.errors as f64 / stats.total_ops as f64
        } else {
            0.0
        };

        {
            let mut all_errs = all_errors.lock().unwrap();
            all_errs.extend(metrics.get_error_details());
        }

        writeln!(
            csv_file,
            "{},{:.2},{},{:.2},{:.2},{:.2},{},{},{},{:.4},{},{}",
            current_writers,
            level_duration,
            stats.total_ops,
            throughput,
            scan_duration,
            stats.latency_mean_ms,
            stats.latency_p50_ms,
            stats.latency_p95_ms,
            stats.latency_p99_ms,
            error_rate,
            stats.successes,
            stats.errors
        )?;

        println!(
            "[{:>6.1}s] Completed: {:.1} ops/sec | Scan: {:.1}s | P99: {}ms | Errors: {:.2}%",
            test_start.elapsed().as_secs_f64(),
            throughput,
            scan_duration,
            stats.latency_p99_ms,
            error_rate * 100.0
        );

        current_writers += args.step_size;
    }

    println!("\n=== Saturation Test Completed ===");
    println!(
        "Total duration: {:.1}s\n",
        test_start.elapsed().as_secs_f64()
    );

    let error_details = all_errors.lock().unwrap();
    let mut error_file = File::create("write_during_scan_errors.txt")?;
    writeln!(error_file, "=== WRITE-DURING-SCAN ERROR ANALYSIS ===")?;
    writeln!(error_file, "Test Date: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(error_file, "Total Errors Captured: {}", error_details.len())?;
    writeln!(error_file)?;
    writeln!(error_file, "=== ERROR DETAILS (Last 10) ===")?;
    writeln!(error_file)?;

    if error_details.is_empty() {
        writeln!(error_file, "No errors detected during write operations.")?;
    } else {
        for (i, err) in error_details.iter().enumerate() {
            writeln!(error_file, "Error {}:", i + 1)?;
            writeln!(error_file, "  Timestamp:    {}", err.timestamp)?;
            writeln!(error_file, "  Operation:    {}", err.operation)?;
            writeln!(error_file, "  Writer ID:    {}", err.writer_id)?;
            writeln!(error_file, "  Object:       {}", err.object_name)?;
            writeln!(error_file, "  Error Type:   {}", err.error_type)?;
            writeln!(error_file, "  Message:      {}", err.error_message)?;
            writeln!(error_file, "  Latency:      {}ms", err.latency_ms)?;
            writeln!(error_file)?;
        }
    }

    println!("[OK] Results written to: write_during_scan_saturation.csv");
    println!("[OK] Error details written to: write_during_scan_errors.txt");
    println!("\n[NEXT] Visualize results:");
    println!("  python examples/s3inventory/plot_write_during_scan.py");

    Ok(())
}
