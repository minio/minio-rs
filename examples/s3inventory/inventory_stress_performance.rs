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

//! Unified stress test: Performance analysis with multiple execution modes.
//!
//! This test provides two execution modes for comprehensive performance analysis:
//!
//! # Saturation Mode
//!
//! Gradually increases concurrent client count to find the saturation point where
//! performance begins to degrade. Outputs detailed metrics to CSV for visualization.
//!
//! **Critical Questions Answered:**
//! - At what concurrent client count does latency exceed 500ms?
//! - Does performance degrade linearly or exponentially with load?
//! - What is the maximum sustainable throughput?
//!
//! **Test Approach:**
//! 1. Start with low concurrency (configurable, default: 5 clients)
//! 2. Run for measurement window (configurable, default: 30 seconds)
//! 3. Record throughput, latency percentiles, error rates
//! 4. Increase concurrency by increment (configurable, default: 5 clients)
//! 5. Repeat until max concurrency or failure threshold
//! 6. Export all metrics to CSV for analysis
//!
//! # Sustained Mode
//!
//! Runs at a sustained high load level for an extended period to detect performance
//! degradation, memory leaks, and resource exhaustion over time. Outputs time-series
//! metrics to CSV for analysis.
//!
//! **Critical Question Answered:**
//! - How long can the system sustain peak load before degrading?
//!
//! **Test Approach:**
//! 1. Run at configured concurrent client count (configurable, default: 50 clients)
//! 2. Sample metrics every interval (configurable, default: 10 seconds)
//! 3. Continue for test duration (configurable, default: 30 minutes)
//! 4. Look for degradation trends over time
//! 5. Export time-series metrics to CSV
//!
//! # Usage
//!
//! ```bash
//! # Saturation mode: Find performance limits
//! cargo run --example inventory_stress_performance -- \
//!   --mode saturation \
//!   --start 5 \
//!   --max 100 \
//!   --step 5 \
//!   --duration 30
//!
//! # Sustained mode: Test long-term stability
//! cargo run --example inventory_stress_performance -- \
//!   --mode sustained \
//!   --clients 50 \
//!   --duration 1800 \
//!   --sample-interval 10
//! ```
//!
//! # Output Files
//!
//! **Saturation Mode:** `inventory_throughput_saturation.csv`
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
//! **Sustained Mode:** `inventory_sustained_load.csv`
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
//! # Configuration
//!
//! **Both Modes:**
//! - `OPERATION_MIX`: Read:List:Status ratio (50:30:20)
//! - `NUM_CONFIGS`: Number of inventory configurations (default: 5)
//!
//! # Requirements
//!
//! - MinIO server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, S3Api};
use minio::s3inventory::{
    DestinationSpec, GetInventoryConfigJson, JobDefinition, JobStatus, ListInventoryConfigsJson,
    ModeSpec, OnOrOff, OutputFormat, PutInventoryConfigResponse, Schedule, VersionsSpec,
};
use rand::Rng;
use std::env;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const NUM_CONFIGS: usize = 5;

#[derive(Debug, Clone, Copy)]
enum TestMode {
    Saturation,
    Sustained,
}

#[derive(Debug, Clone)]
struct SaturationConfig {
    start_clients: usize,
    max_clients: usize,
    client_step: usize,
    measurement_duration_secs: u64,
}

impl Default for SaturationConfig {
    fn default() -> Self {
        Self {
            start_clients: 5,
            max_clients: 100,
            client_step: 5,
            measurement_duration_secs: 30,
        }
    }
}

#[derive(Debug, Clone)]
struct SustainedConfig {
    concurrent_clients: usize,
    test_duration_secs: u64,
    sample_interval_secs: u64,
}

impl Default for SustainedConfig {
    fn default() -> Self {
        Self {
            concurrent_clients: 50,
            test_duration_secs: 1800,
            sample_interval_secs: 10,
        }
    }
}

#[derive(Debug, Clone)]
struct TestConfig {
    mode: TestMode,
    saturation: SaturationConfig,
    sustained: SustainedConfig,
}

#[derive(Debug, Clone)]
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

        let window_duration = ops
            .last()
            .unwrap()
            .timestamp
            .duration_since(ops.first().unwrap().timestamp);
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

        let window_duration = ops
            .last()
            .unwrap()
            .timestamp
            .duration_since(ops.first().unwrap().timestamp);
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
            self.op_counter.load(Ordering::Relaxed) + self.error_counter.load(Ordering::Relaxed),
            self.error_counter.load(Ordering::Relaxed),
        )
    }

    fn reset_window(&self) {
        self.operations.lock().unwrap().clear();
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

#[derive(Debug, Clone)]
struct WindowStats {
    window_ops: usize,
    window_throughput: f64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    #[allow(dead_code)]
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

async fn setup_test_infrastructure(
    client: &MinioClient,
    bucket_prefix: &str,
    dest_bucket: &BucketName,
) -> Result<Vec<(BucketName, String)>, Box<dyn std::error::Error + Send + Sync>> {
    println!("Setting up test infrastructure...");
    let _ = client
        .create_bucket(dest_bucket.clone())
        .unwrap()
        .build()
        .send()
        .await;

    let mut config_info = Vec::new();
    for i in 0..NUM_CONFIGS {
        let bucket = BucketName::new(format!("{}-{}", bucket_prefix, i)).unwrap();
        let job_id = format!("config-{}", i);

        let _ = client.create_bucket(bucket.clone()).unwrap().build().send().await;

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
    Ok(config_info)
}

async fn run_saturation_mode(
    config: &SaturationConfig,
    base_url: BaseUrl,
    static_provider: StaticProvider,
    client: MinioClient,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== S3 Inventory Stress Test: Throughput Saturation Analysis ===\n");
    println!("Configuration:");
    println!("  Start clients:        {}", config.start_clients);
    println!("  Client step:          {}", config.client_step);
    println!("  Max clients:          {}", config.max_clients);
    println!(
        "  Measurement duration: {} seconds",
        config.measurement_duration_secs
    );
    println!("  Operation mix:        50% read, 30% list, 20% status\n");

    let dest_bucket = BucketName::new("saturation-test-reports").unwrap();
    let config_info = setup_test_infrastructure(&client, "saturation-test", &dest_bucket).await?;

    let csv_filename = "inventory_throughput_saturation.csv";
    let mut csv_file = File::create(csv_filename)?;
    writeln!(
        csv_file,
        "concurrent_clients,elapsed_secs,total_ops,throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,success_count,error_count"
    )?;

    println!("Running saturation test...\n");
    let test_start = Instant::now();
    let mut results = Vec::new();

    for num_clients in (config.start_clients..=config.max_clients).step_by(config.client_step) {
        println!("[Clients: {}] Starting measurement window...", num_clients);

        let collector = Arc::new(MetricsCollector::new());
        let stop_signal = Arc::new(AtomicBool::new(false));
        let mut tasks = JoinSet::new();

        for _ in 0..num_clients {
            let client_clone =
                MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;
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

        tokio::time::sleep(Duration::from_secs(config.measurement_duration_secs)).await;

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
            println!("P99 latency exceeded 2000ms - stopping test early");
            break;
        }

        if stats.error_rate > 0.1 {
            println!("Error rate exceeded 10% - stopping test early");
            break;
        }
    }

    println!("\n=== Test Complete ===");
    println!("Results written to: {}", csv_filename);
    println!("\nKey Findings:");

    if let Some((clients, _)) = results.iter().find(|(_, s)| s.latency_p99_ms > 500) {
        println!(
            "  - P99 latency exceeded 500ms at {} concurrent clients",
            clients
        );
    } else {
        println!("  - P99 latency remained below 500ms for all tested loads");
    }

    let max_throughput = results
        .iter()
        .map(|(_, s)| s.throughput)
        .fold(0.0, f64::max);
    if let Some((clients, _)) = results
        .iter()
        .find(|(_, s)| s.throughput >= max_throughput * 0.95)
    {
        println!(
            "  - Peak throughput ({:.2} ops/sec) reached at {} clients",
            max_throughput, clients
        );
    }

    let throughput_growth = results
        .iter()
        .map(|(c, s)| s.throughput / (*c as f64))
        .collect::<Vec<_>>();

    let variance = throughput_growth
        .iter()
        .map(|x| (x - throughput_growth[0]).abs() / throughput_growth[0])
        .sum::<f64>()
        / throughput_growth.len() as f64;

    if variance < 0.2 {
        println!(
            "  - Performance scales linearly with client count (variance: {:.2}%)",
            variance * 100.0
        );
    } else {
        println!(
            "  - Performance degradation is non-linear (variance: {:.2}%)",
            variance * 100.0
        );
    }

    Ok(())
}

async fn run_sustained_mode(
    config: &SustainedConfig,
    base_url: BaseUrl,
    static_provider: StaticProvider,
    client: MinioClient,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== S3 Inventory Stress Test: Sustained High Load Analysis ===\n");
    println!("Configuration:");
    println!("  Concurrent clients:   {}", config.concurrent_clients);
    println!(
        "  Test duration:        {} seconds ({:.1} minutes)",
        config.test_duration_secs,
        config.test_duration_secs as f64 / 60.0
    );
    println!(
        "  Sample interval:      {} seconds",
        config.sample_interval_secs
    );
    println!("  Operation mix:        50% read, 30% list, 20% status\n");

    let dest_bucket = BucketName::new("sustained-load-reports").unwrap();
    let config_info = setup_test_infrastructure(&client, "sustained-load", &dest_bucket).await?;

    let csv_filename = "inventory_sustained_load.csv";
    let mut csv_file = File::create(csv_filename)?;
    writeln!(
        csv_file,
        "elapsed_secs,sample_window_ops,window_throughput,cumulative_ops,cumulative_throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,cumulative_error_rate"
    )?;

    println!("Starting sustained load test...\n");
    let test_start = Instant::now();
    let collector = Arc::new(MetricsCollector::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for _ in 0..config.concurrent_clients {
        let client_clone =
            MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;
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

    println!("Test running for {} seconds...", config.test_duration_secs);
    println!(
        "Progress updates every {} seconds:\n",
        config.sample_interval_secs
    );

    let mut sample_count = 0;
    let target_samples = config.test_duration_secs / config.sample_interval_secs;

    while sample_count < target_samples {
        tokio::time::sleep(Duration::from_secs(config.sample_interval_secs)).await;
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

        println!(
            "[{:>4}s] Window: {} ops ({:.1} ops/s) | P99: {} ms | Errors: {:.2}%",
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
            println!("\nError rate exceeded 20% - stopping test early");
            break;
        }

        if window_stats.latency_p99_ms > 5000 {
            println!("\nP99 latency exceeded 5000ms - stopping test early");
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
    println!(
        "  Test duration:        {:.1} minutes",
        final_elapsed / 60.0
    );
    println!("  Total operations:     {}", final_ops);
    println!("  Total errors:         {}", final_errors);
    println!(
        "  Average throughput:   {:.2} ops/sec",
        final_ops as f64 / final_elapsed
    );
    println!(
        "  Overall error rate:   {:.2}%",
        (final_errors as f64 / final_ops as f64) * 100.0
    );

    Ok(())
}

fn parse_args() -> Result<TestConfig, Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        return Err("Usage: --mode <saturation|sustained> [options]".into());
    }

    let mode_str = &args[2];
    let mode = match mode_str.as_str() {
        "saturation" => TestMode::Saturation,
        "sustained" => TestMode::Sustained,
        _ => {
            return Err(format!(
                "Invalid mode: {}. Use 'saturation' or 'sustained'",
                mode_str
            )
            .into());
        }
    };

    let mut saturation = SaturationConfig::default();
    let mut sustained = SustainedConfig::default();

    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--start" => {
                if i + 1 < args.len() {
                    saturation.start_clients = args[i + 1].parse()?;
                    i += 2;
                } else {
                    return Err("--start requires a value".into());
                }
            }
            "--max" => {
                if i + 1 < args.len() {
                    saturation.max_clients = args[i + 1].parse()?;
                    i += 2;
                } else {
                    return Err("--max requires a value".into());
                }
            }
            "--step" => {
                if i + 1 < args.len() {
                    saturation.client_step = args[i + 1].parse()?;
                    i += 2;
                } else {
                    return Err("--step requires a value".into());
                }
            }
            "--duration" => {
                if i + 1 < args.len() {
                    match mode {
                        TestMode::Saturation => {
                            saturation.measurement_duration_secs = args[i + 1].parse()?
                        }
                        TestMode::Sustained => {
                            sustained.test_duration_secs = args[i + 1].parse()?
                        }
                    }
                    i += 2;
                } else {
                    return Err("--duration requires a value".into());
                }
            }
            "--clients" => {
                if i + 1 < args.len() {
                    sustained.concurrent_clients = args[i + 1].parse()?;
                    i += 2;
                } else {
                    return Err("--clients requires a value".into());
                }
            }
            "--sample-interval" => {
                if i + 1 < args.len() {
                    sustained.sample_interval_secs = args[i + 1].parse()?;
                    i += 2;
                } else {
                    return Err("--sample-interval requires a value".into());
                }
            }
            _ => {
                return Err(format!("Unknown argument: {}", args[i]).into());
            }
        }
    }

    Ok(TestConfig {
        mode,
        saturation,
        sustained,
    })
}

fn print_usage() {
    println!("S3 Inventory Performance Stress Test\n");
    println!("Usage:");
    println!(
        "  cargo run --example inventory_stress_performance -- --mode <saturation|sustained> [options]\n"
    );
    println!("Saturation Mode Options:");
    println!("  --start <N>            Starting number of concurrent clients (default: 5)");
    println!("  --max <N>              Maximum number of concurrent clients (default: 100)");
    println!("  --step <N>             Client increment per round (default: 5)");
    println!("  --duration <secs>      Measurement duration per level (default: 30)\n");
    println!("Sustained Mode Options:");
    println!("  --clients <N>          Number of concurrent clients (default: 50)");
    println!("  --duration <secs>      Total test duration (default: 1800)");
    println!("  --sample-interval <s>  Metrics sampling interval (default: 10)\n");
    println!("Examples:");
    println!("  # Saturation mode");
    println!(
        "  cargo run --example inventory_stress_performance -- --mode saturation --start 5 --max 100 --step 5 --duration 30\n"
    );
    println!("  # Sustained mode");
    println!(
        "  cargo run --example inventory_stress_performance -- --mode sustained --clients 50 --duration 1800 --sample-interval 10"
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}\n", e);
            print_usage();
            std::process::exit(1);
        }
    };

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;

    match config.mode {
        TestMode::Saturation => {
            run_saturation_mode(&config.saturation, base_url, static_provider, client).await?;
        }
        TestMode::Sustained => {
            run_sustained_mode(&config.sustained, base_url, static_provider, client).await?;
        }
    }

    Ok(())
}
