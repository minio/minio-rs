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

//! Stress test: Sustained high load analysis.
//!
//! This test runs at a sustained high load level for an extended period to
//! detect performance degradation, memory leaks, and resource exhaustion over
//! time. Outputs time-series metrics to CSV for analysis.
//!
//! # Critical Question Answered
//!
//! 7. How long can the system sustain peak load before degrading?
//!
//! # Test Approach
//!
//! 1. Run at configured concurrent client count (e.g., 50 clients)
//! 2. Sample metrics every SAMPLE_INTERVAL_SECS (e.g., 10 seconds)
//! 3. Continue for TEST_DURATION_SECS (e.g., 30 minutes)
//! 4. Look for degradation trends over time
//! 5. Export time-series metrics to CSV
//!
//! # Configuration
//!
//! - `CONCURRENT_CLIENTS`: Number of concurrent clients (default: 50)
//! - `TEST_DURATION_SECS`: Total test duration (default: 1800 = 30 minutes)
//! - `SAMPLE_INTERVAL_SECS`: Metrics sampling interval (default: 10)
//! - `OPERATION_MIX`: Read:List:Write ratio (50:30:20)
//!
//! # Output
//!
//! Creates `inventory_sustained_load.csv` with columns:
//! - elapsed_secs: Time since test start
//! - sample_window_ops: Operations in this sample window
//! - window_throughput: Throughput for this window (ops/sec)
//! - cumulative_ops: Total operations since start
//! - cumulative_throughput: Average throughput since start
//! - latency_mean_ms: Mean latency for this window
//! - latency_p50_ms: P50 latency for this window
//! - latency_p95_ms: P95 latency for this window
//! - latency_p99_ms: P99 latency for this window
//! - error_rate: Error rate for this window
//! - cumulative_error_rate: Average error rate since start
//!
//! # Requirements
//!
//! - MinIO server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin

use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::response::CreateBucketResponse;
use minio::s3::types::{BucketName, S3Api};
use minio::s3::MinioClient;
use minio::s3inventory::{
    DestinationSpec, GetInventoryConfigJson, JobDefinition, JobStatus, ListInventoryConfigsJson,
    ModeSpec, OnOrOff, OutputFormat, PutInventoryConfigResponse, Schedule, VersionsSpec,
};
use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const CONCURRENT_CLIENTS: usize = 50;
const TEST_DURATION_SECS: u64 = 1800;
const SAMPLE_INTERVAL_SECS: u64 = 10;
const NUM_CONFIGS: usize = 5;

#[derive(Debug, Clone)]
struct OperationMetric {
    timestamp: Instant,
    duration_ms: u64,
    success: bool,
}

struct MetricsCollector {
    operations: Arc<Mutex<Vec<OperationMetric>>>,
    cumulative_ops: AtomicU64,
    cumulative_errors: AtomicU64,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
            cumulative_ops: AtomicU64::new(0),
            cumulative_errors: AtomicU64::new(0),
        }
    }

    fn record(&self, start: Instant, success: bool) {
        let duration_ms = start.elapsed().as_millis() as u64;

        self.cumulative_ops.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.cumulative_errors.fetch_add(1, Ordering::Relaxed);
        }

        let mut ops = self.operations.lock().unwrap();
        ops.push(OperationMetric {
            timestamp: start,
            duration_ms,
            success,
        });
    }

    fn compute_window_stats(&self) -> WindowStats {
        let ops = self.operations.lock().unwrap();
        let window_ops = ops.len();

        if ops.is_empty() {
            return WindowStats {
                window_ops: 0,
                window_throughput: 0.0,
                latency_mean_ms: 0.0,
                latency_p50_ms: 0,
                latency_p95_ms: 0,
                latency_p99_ms: 0,
                window_error_count: 0,
                window_error_rate: 0.0,
            };
        }

        let window_errors = ops.iter().filter(|m| !m.success).count();

        let mut latencies: Vec<u64> = ops.iter().map(|m| m.duration_ms).collect();
        latencies.sort_unstable();

        let latency_mean_ms = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
        let latency_p50_ms = latencies[latencies.len() * 50 / 100];
        let latency_p95_ms = latencies[latencies.len() * 95 / 100];
        let latency_p99_ms = latencies[latencies.len() * 99 / 100];

        let window_duration = ops.last().unwrap().timestamp.duration_since(ops.first().unwrap().timestamp);
        let window_throughput = if window_duration.as_secs_f64() > 0.0 {
            window_ops as f64 / window_duration.as_secs_f64()
        } else {
            0.0
        };

        let window_error_rate = window_errors as f64 / window_ops as f64;

        WindowStats {
            window_ops,
            window_throughput,
            latency_mean_ms,
            latency_p50_ms,
            latency_p95_ms,
            latency_p99_ms,
            window_error_count: window_errors,
            window_error_rate,
        }
    }

    fn get_cumulative_stats(&self) -> (u64, u64) {
        (
            self.cumulative_ops.load(Ordering::Relaxed),
            self.cumulative_errors.load(Ordering::Relaxed),
        )
    }

    fn reset_window(&self) {
        self.operations.lock().unwrap().clear();
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct WindowStats {
    window_ops: usize,
    window_throughput: f64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    window_error_count: usize,
    window_error_rate: f64,
}

async fn client_task(
    client: MinioClient,
    config_info: Vec<(BucketName, String)>,
    collector: Arc<MetricsCollector>,
    stop_signal: Arc<AtomicBool>,
) {
    while !stop_signal.load(Ordering::Relaxed) {
        let (operation, config_idx, sleep_ms) = {
            let mut rng = rand::rng();
            let op = rng.random_range(0..10);
            let idx = rng.random_range(0..config_info.len());
            let sleep = rng.random_range(10..30);
            (op, idx, sleep)
        };

        let (bucket, job_id) = &config_info[config_idx];

        let start = Instant::now();

        let success = if operation < 5 {
            let builder = match client.get_inventory_config(bucket.clone(), job_id) {
                Ok(b) => b,
                Err(_) => continue,
            };
            match builder.build().send().await {
                Ok(resp) => {
                    let config: Result<GetInventoryConfigJson, _> = resp.inventory_config();
                    config.is_ok()
                }
                Err(_) => false,
            }
        } else if operation < 8 {
            let builder = match client.list_inventory_configs(bucket.clone()) {
                Ok(b) => b,
                Err(_) => continue,
            };
            match builder.build().send().await {
                Ok(resp) => {
                    let configs: Result<ListInventoryConfigsJson, _> = resp.configs();
                    configs.is_ok()
                }
                Err(_) => false,
            }
        } else {
            let builder = match client.get_inventory_job_status(bucket.clone(), job_id) {
                Ok(b) => b,
                Err(_) => continue,
            };
            match builder.build().send().await {
                Ok(resp) => {
                    let status: Result<JobStatus, _> = resp.status();
                    status.is_ok()
                }
                Err(_) => false,
            }
        };

        collector.record(start, success);

        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== S3 Inventory Stress Test: Sustained High Load Analysis ===\n");
    println!("Configuration:");
    println!("  Concurrent clients:   {}", CONCURRENT_CLIENTS);
    println!("  Test duration:        {} seconds ({:.1} minutes)", TEST_DURATION_SECS, TEST_DURATION_SECS as f64 / 60.0);
    println!("  Sample interval:      {} seconds", SAMPLE_INTERVAL_SECS);
    println!("  Operation mix:        50% read, 30% list, 20% status\n");

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(
        base_url.clone(),
        Some(static_provider.clone()),
        None,
        None,
    )?;

    let dest_bucket = BucketName::new("sustained-load-reports").unwrap();
    println!("Step 1: Creating test infrastructure...");
    let _: CreateBucketResponse = client.create_bucket(dest_bucket.clone()).build().send().await?;

    let mut config_info = Vec::new();
    for i in 0..NUM_CONFIGS {
        let bucket = BucketName::new(&format!("sustained-load-{}", i)).unwrap();
        let job_id = format!("config-{}", i);

        let _: CreateBucketResponse = client.create_bucket(bucket.clone()).build().send().await?;

        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: job_id.clone(),
            destination: DestinationSpec {
                bucket: dest_bucket.to_string(),
                prefix: Some(format!("config-{}/", i)),
                format: OutputFormat::CSV,
                compression: OnOrOff::On,
                max_file_size_hint: None,
            },
            schedule: Schedule::Daily,
            mode: ModeSpec::Fast,
            versions: VersionsSpec::Current,
            include_fields: vec![],
            filters: None,
        };

        let _: PutInventoryConfigResponse = client
            .put_inventory_config(bucket.clone(), &job_id, job)?
            .build()
            .send()
            .await?;

        config_info.push((bucket, job_id));
    }
    println!("  Created {} test buckets and configs\n", NUM_CONFIGS);

    let csv_filename = "inventory_sustained_load.csv";
    let mut csv_file = File::create(csv_filename)?;
    writeln!(
        csv_file,
        "elapsed_secs,sample_window_ops,window_throughput,cumulative_ops,cumulative_throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,cumulative_error_rate"
    )?;

    println!("Step 2: Starting sustained load test...\n");
    let test_start = Instant::now();
    let collector = Arc::new(MetricsCollector::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for _ in 0..CONCURRENT_CLIENTS {
        let client_clone = MinioClient::new(
            base_url.clone(),
            Some(static_provider.clone()),
            None,
            None,
        )?;
        let config_info_clone = config_info.clone();
        let collector_clone = Arc::clone(&collector);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            client_task(
                client_clone,
                config_info_clone,
                collector_clone,
                stop_signal_clone,
            )
            .await;
        });
    }

    println!("Test running for {} seconds...", TEST_DURATION_SECS);
    println!("Progress updates every {} seconds:\n", SAMPLE_INTERVAL_SECS);

    let mut sample_count = 0;
    let target_samples = TEST_DURATION_SECS / SAMPLE_INTERVAL_SECS;

    while sample_count < target_samples {
        tokio::time::sleep(Duration::from_secs(SAMPLE_INTERVAL_SECS)).await;
        sample_count += 1;

        let elapsed_secs = test_start.elapsed().as_secs();
        let window_stats = collector.compute_window_stats();
        let (cumulative_ops, cumulative_errors) = collector.get_cumulative_stats();

        let cumulative_throughput = cumulative_ops as f64 / elapsed_secs as f64;
        let cumulative_error_rate = if cumulative_ops > 0 {
            cumulative_errors as f64 / cumulative_ops as f64
        } else {
            0.0
        };

        println!("[{:>4}s] Window: {} ops ({:.1} ops/s) | P99: {} ms | Errors: {:.2}%",
            elapsed_secs,
            window_stats.window_ops,
            window_stats.window_throughput,
            window_stats.latency_p99_ms,
            window_stats.window_error_rate * 100.0
        );

        writeln!(
            csv_file,
            "{},{},{:.2},{},{:.2},{:.2},{},{},{},{:.4},{:.4}",
            elapsed_secs,
            window_stats.window_ops,
            window_stats.window_throughput,
            cumulative_ops,
            cumulative_throughput,
            window_stats.latency_mean_ms,
            window_stats.latency_p50_ms,
            window_stats.latency_p95_ms,
            window_stats.latency_p99_ms,
            window_stats.window_error_rate,
            cumulative_error_rate
        )?;
        csv_file.flush()?;

        collector.reset_window();

        if window_stats.window_error_rate > 0.2 {
            println!("\n⚠️  Error rate exceeded 20% - stopping test early");
            break;
        }

        if window_stats.latency_p99_ms > 5000 {
            println!("\n⚠️  P99 latency exceeded 5000ms - stopping test early");
            break;
        }
    }

    println!("\nStopping all client tasks...");
    stop_signal.store(true, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Client task error: {}", e);
        }
    }

    let (final_ops, final_errors) = collector.get_cumulative_stats();
    let final_elapsed = test_start.elapsed().as_secs_f64();

    println!("\n=== Test Complete ===");
    println!("Results written to: {}", csv_filename);
    println!("\nFinal Statistics:");
    println!("  Test duration:        {:.1} minutes", final_elapsed / 60.0);
    println!("  Total operations:     {}", final_ops);
    println!("  Total errors:         {}", final_errors);
    println!("  Average throughput:   {:.2} ops/sec", final_ops as f64 / final_elapsed);
    println!("  Overall error rate:   {:.2}%", (final_errors as f64 / final_ops as f64) * 100.0);

    println!("\nRun visualization script:");
    println!("  python plot_sustained_load.py");

    Ok(())
}
