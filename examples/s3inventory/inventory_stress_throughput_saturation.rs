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

//! Stress test: Throughput saturation analysis.
//!
//! This test gradually increases concurrent client count to find the saturation
//! point where performance begins to degrade. Outputs detailed metrics to CSV
//! for visualization and analysis.
//!
//! # Critical Questions Answered
//!
//! 1. At what concurrent client count does latency exceed 500ms?
//! 3. Does performance degrade linearly or exponentially with load?
//!
//! # Test Approach
//!
//! 1. Start with low concurrency (5 clients)
//! 2. Run for measurement window (30 seconds)
//! 3. Record throughput, latency percentiles, error rates
//! 4. Increase concurrency by increment (5 clients)
//! 5. Repeat until max concurrency or failure threshold
//! 6. Export all metrics to CSV for analysis
//!
//! # Configuration
//!
//! - `START_CLIENTS`: Initial concurrent clients (default: 5)
//! - `CLIENT_INCREMENT`: Clients to add each round (default: 5)
//! - `MAX_CLIENTS`: Maximum concurrent clients (default: 100)
//! - `MEASUREMENT_WINDOW_SECS`: Duration per concurrency level (default: 30)
//! - `OPERATION_MIX`: Read:List:Write ratio (50:30:20)
//!
//! # Output
//!
//! Creates `inventory_throughput_saturation.csv` with columns:
//! - concurrent_clients: Number of concurrent clients
//! - elapsed_secs: Time since test start
//! - total_ops: Total operations completed
//! - throughput: Operations per second
//! - latency_mean_ms: Mean latency
//! - latency_p50_ms: Median latency
//! - latency_p95_ms: 95th percentile latency
//! - latency_p99_ms: 99th percentile latency
//! - error_rate: Error rate (0.0-1.0)
//! - success_count: Successful operations
//! - error_count: Failed operations
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

const START_CLIENTS: usize = 5;
const CLIENT_INCREMENT: usize = 5;
const MAX_CLIENTS: usize = 100;
const MEASUREMENT_WINDOW_SECS: u64 = 30;
const NUM_CONFIGS: usize = 5;

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct OperationMetric {
    timestamp: Instant,
    duration_ms: u64,
    success: bool,
}

struct MetricsCollector {
    operations: Arc<Mutex<Vec<OperationMetric>>>,
    op_counter: AtomicU64,
    error_counter: AtomicU64,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
            op_counter: AtomicU64::new(0),
            error_counter: AtomicU64::new(0),
        }
    }

    fn record(&self, start: Instant, success: bool) {
        let duration_ms = start.elapsed().as_millis() as u64;

        if success {
            self.op_counter.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error_counter.fetch_add(1, Ordering::Relaxed);
        }

        let mut ops = self.operations.lock().unwrap();
        ops.push(OperationMetric {
            timestamp: start,
            duration_ms,
            success,
        });
    }

    fn compute_stats(&self, test_start: Instant) -> AggregateStats {
        let ops = self.operations.lock().unwrap();
        let total_ops = ops.len() as u64;
        let success_count = self.op_counter.load(Ordering::Relaxed);
        let error_count = self.error_counter.load(Ordering::Relaxed);

        if ops.is_empty() {
            return AggregateStats {
                total_ops: 0,
                throughput: 0.0,
                latency_mean_ms: 0.0,
                latency_p50_ms: 0,
                latency_p95_ms: 0,
                latency_p99_ms: 0,
                error_rate: 0.0,
                success_count: 0,
                error_count: 0,
                elapsed_secs: test_start.elapsed().as_secs_f64(),
            };
        }

        let mut latencies: Vec<u64> = ops.iter().map(|m| m.duration_ms).collect();
        latencies.sort_unstable();

        let latency_mean_ms = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
        let latency_p50_ms = latencies[latencies.len() * 50 / 100];
        let latency_p95_ms = latencies[latencies.len() * 95 / 100];
        let latency_p99_ms = latencies[latencies.len() * 99 / 100];

        let window_duration = ops.last().unwrap().timestamp.duration_since(ops.first().unwrap().timestamp);
        let throughput = if window_duration.as_secs_f64() > 0.0 {
            total_ops as f64 / window_duration.as_secs_f64()
        } else {
            0.0
        };

        let error_rate = if total_ops > 0 {
            error_count as f64 / total_ops as f64
        } else {
            0.0
        };

        AggregateStats {
            total_ops,
            throughput,
            latency_mean_ms,
            latency_p50_ms,
            latency_p95_ms,
            latency_p99_ms,
            error_rate,
            success_count,
            error_count,
            elapsed_secs: test_start.elapsed().as_secs_f64(),
        }
    }

    #[allow(dead_code)]
    fn reset(&self) {
        self.operations.lock().unwrap().clear();
        self.op_counter.store(0, Ordering::Relaxed);
        self.error_counter.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
struct AggregateStats {
    total_ops: u64,
    throughput: f64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    error_rate: f64,
    success_count: u64,
    error_count: u64,
    elapsed_secs: f64,
}

async fn client_task(
    client: MinioClient,
    config_info: Vec<(String, String)>,
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
    println!("=== S3 Inventory Stress Test: Throughput Saturation Analysis ===\n");
    println!("Configuration:");
    println!("  Start clients:        {}", START_CLIENTS);
    println!("  Client increment:     {}", CLIENT_INCREMENT);
    println!("  Max clients:          {}", MAX_CLIENTS);
    println!("  Measurement window:   {} seconds", MEASUREMENT_WINDOW_SECS);
    println!("  Operation mix:        50% read, 30% list, 20% status\n");

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(
        base_url.clone(),
        Some(static_provider.clone()),
        None,
        None,
    )?;

    let dest_bucket_str = "saturation-test-reports";
    let dest_bucket = BucketName::new(dest_bucket_str).unwrap();
    println!("Step 1: Creating test infrastructure...");
    let _: CreateBucketResponse = client.create_bucket(dest_bucket).build().send().await?;

    let mut config_info: Vec<(BucketName, String)> = Vec::new();
    for i in 0..NUM_CONFIGS {
        let bucket_str = format!("saturation-test-{}", i);
        let bucket = BucketName::new(&bucket_str).unwrap();
        let job_id = format!("config-{}", i);

        let _: CreateBucketResponse = client.create_bucket(bucket.clone()).build().send().await?;

        let job = JobDefinition {
            api_version: "v1".to_string(),
            id: job_id.clone(),
            destination: DestinationSpec {
                bucket: dest_bucket_str.to_string(),
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

    let csv_filename = "inventory_throughput_saturation.csv";
    let mut csv_file = File::create(csv_filename)?;
    writeln!(
        csv_file,
        "concurrent_clients,elapsed_secs,total_ops,throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,success_count,error_count"
    )?;

    println!("Step 2: Running saturation test...\n");
    let test_start = Instant::now();
    let mut results = Vec::new();

    for num_clients in (START_CLIENTS..=MAX_CLIENTS).step_by(CLIENT_INCREMENT) {
        println!("[Clients: {}] Starting measurement window...", num_clients);

        let collector = Arc::new(MetricsCollector::new());
        let stop_signal = Arc::new(AtomicBool::new(false));
        let mut tasks = JoinSet::new();

        for _ in 0..num_clients {
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

        tokio::time::sleep(Duration::from_secs(MEASUREMENT_WINDOW_SECS)).await;

        stop_signal.store(true, Ordering::Relaxed);

        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                eprintln!("Client task error: {}", e);
            }
        }

        let stats = collector.compute_stats(test_start);

        println!("[Clients: {}] Results:", num_clients);
        println!("  Total ops:      {}", stats.total_ops);
        println!("  Throughput:     {:.2} ops/sec", stats.throughput);
        println!("  Latency mean:   {:.2} ms", stats.latency_mean_ms);
        println!("  Latency P50:    {} ms", stats.latency_p50_ms);
        println!("  Latency P95:    {} ms", stats.latency_p95_ms);
        println!("  Latency P99:    {} ms", stats.latency_p99_ms);
        println!("  Error rate:     {:.2}%\n", stats.error_rate * 100.0);

        writeln!(
            csv_file,
            "{},{:.2},{},{:.2},{:.2},{},{},{},{:.4},{},{}",
            num_clients,
            stats.elapsed_secs,
            stats.total_ops,
            stats.throughput,
            stats.latency_mean_ms,
            stats.latency_p50_ms,
            stats.latency_p95_ms,
            stats.latency_p99_ms,
            stats.error_rate,
            stats.success_count,
            stats.error_count
        )?;
        csv_file.flush()?;

        results.push((num_clients, stats.clone()));

        if stats.latency_p99_ms > 2000 {
            println!("⚠️  P99 latency exceeded 2000ms - stopping test early");
            break;
        }

        if stats.error_rate > 0.1 {
            println!("⚠️  Error rate exceeded 10% - stopping test early");
            break;
        }
    }

    println!("\n=== Test Complete ===");
    println!("Results written to: {}", csv_filename);
    println!("\nKey Findings:");

    if let Some((clients, _)) = results.iter().find(|(_, s)| s.latency_p99_ms > 500) {
        println!("  • P99 latency exceeded 500ms at {} concurrent clients", clients);
    } else {
        println!("  • P99 latency remained below 500ms for all tested loads");
    }

    let max_throughput = results.iter().map(|(_, s)| s.throughput).fold(0.0, f64::max);
    if let Some((clients, _)) = results.iter().find(|(_, s)| s.throughput >= max_throughput * 0.95) {
        println!("  • Peak throughput ({:.2} ops/sec) reached at {} clients", max_throughput, clients);
    }

    let throughput_growth = results.iter()
        .map(|(c, s)| s.throughput / (*c as f64))
        .collect::<Vec<_>>();

    let variance = throughput_growth.iter()
        .map(|x| (x - throughput_growth[0]).abs() / throughput_growth[0])
        .sum::<f64>() / throughput_growth.len() as f64;

    if variance < 0.2 {
        println!("  • Performance scales linearly with client count (variance: {:.2}%)", variance * 100.0);
    } else {
        println!("  • Performance degradation is non-linear (variance: {:.2}%)", variance * 100.0);
    }

    println!("\nRun visualization script:");
    println!("  python plot_saturation.py");

    Ok(())
}
