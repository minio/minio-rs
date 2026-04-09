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

//! Stress test: Write-heavy concurrent configuration modifications with saturation analysis.
//!
//! This stress test evaluates write contention and conflict resolution by gradually
//! increasing the number of concurrent writers competing to modify the same inventory
//! configurations. It measures throughput saturation, latency degradation, and error
//! rates under increasing write load.
//!
//! # Test Scenario
//!
//! 1. Create initial inventory configurations (shared write targets)
//! 2. Gradually increase concurrent writers from START_CLIENTS to MAX_CLIENTS
//! 3. Each load level runs for LEVEL_DURATION_SECS seconds
//! 4. Writers perform: UpdateConfig, ReadConfig, ListConfigs, DeleteAndRecreate
//! 5. Collect throughput, latency percentiles (P50/P95/P99), and error rates
//! 6. Generate CSV output for saturation analysis visualization
//!
//! # Configuration (CLI Arguments)
//!
//! - `--start-clients`: Initial concurrent writers (default: 5)
//! - `--max-clients`: Maximum concurrent writers (default: 50)
//! - `--step`: Client increment per level (default: 5)
//! - `--level-duration`: Seconds per load level (default: 30)
//! - `--num-configs`: Shared inventory configs (default: 3)
//!
//! # Expected Behavior
//!
//! - Throughput increases initially, then plateaus/saturates
//! - Latency increases as write contention grows
//! - Error rates may increase due to conflicts (last-write-wins semantics)
//! - Demonstrates system behavior under write-heavy contention
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
    DestinationSpec, JobDefinition, ModeSpec, OnOrOff, OutputFormat, PutInventoryConfigResponse,
    Schedule, VersionsSpec,
};
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Clone)]
struct TestConfig {
    start_clients: usize,
    max_clients: usize,
    step: usize,
    level_duration_secs: u64,
    num_configs: usize,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            start_clients: 5,
            max_clients: 50,
            step: 5,
            level_duration_secs: 30,
            num_configs: 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    UpdateConfig,
    ReadConfig,
    ListConfigs,
    DeleteAndRecreate,
}

struct OperationMetric {
    latency_ms: u64,
    success: bool,
    _timestamp: Instant,
}

#[derive(Debug, Clone)]
struct ErrorDetail {
    timestamp: String,
    operation: String,
    bucket: String,
    job_id: String,
    error_type: String,
    error_message: String,
    latency_ms: u64,
}

struct MetricsCollector {
    operations: Arc<Mutex<Vec<OperationMetric>>>,
    op_counter: AtomicU64,
    error_counter: AtomicU64,
    unexpected_errors: Arc<Mutex<Vec<ErrorDetail>>>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            operations: Arc::new(Mutex::new(Vec::new())),
            op_counter: AtomicU64::new(0),
            error_counter: AtomicU64::new(0),
            unexpected_errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record_operation(&self, latency_ms: u64, success: bool) {
        if success {
            self.op_counter.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error_counter.fetch_add(1, Ordering::Relaxed);
        }

        let mut ops = self.operations.lock().unwrap();
        ops.push(OperationMetric {
            latency_ms,
            success,
            _timestamp: Instant::now(),
        });
    }

    fn compute_stats(&self, test_start: Instant) -> AggregateStats {
        let ops = self.operations.lock().unwrap();
        let elapsed_secs = test_start.elapsed().as_secs_f64();

        if ops.is_empty() {
            return AggregateStats::default();
        }

        let mut latencies: Vec<u64> = ops.iter().map(|m| m.latency_ms).collect();
        latencies.sort_unstable();

        let total_ops = ops.len() as u64;
        let success_count = ops.iter().filter(|m| m.success).count() as u64;
        let error_count = total_ops - success_count;

        AggregateStats {
            _elapsed_secs: elapsed_secs,
            total_ops,
            throughput: total_ops as f64 / elapsed_secs,
            latency_mean_ms: latencies.iter().sum::<u64>() as f64 / latencies.len() as f64,
            latency_p50_ms: Self::percentile(&latencies, 50.0),
            latency_p95_ms: Self::percentile(&latencies, 95.0),
            latency_p99_ms: Self::percentile(&latencies, 99.0),
            error_rate: error_count as f64 / total_ops as f64,
            success_count,
            error_count,
        }
    }

    fn percentile(sorted_values: &[u64], percentile: f64) -> u64 {
        if sorted_values.is_empty() {
            return 0;
        }
        let index = ((percentile / 100.0) * (sorted_values.len() - 1) as f64).round() as usize;
        sorted_values[index.min(sorted_values.len() - 1)]
    }

    fn capture_unexpected_error(&self, detail: ErrorDetail) {
        let mut errors = self.unexpected_errors.lock().unwrap();
        errors.push(detail);
        if errors.len() > 10 {
            errors.remove(0);
        }
    }

    fn get_unexpected_errors(&self) -> Vec<ErrorDetail> {
        self.unexpected_errors.lock().unwrap().clone()
    }
}

#[derive(Debug, Default)]
struct AggregateStats {
    _elapsed_secs: f64,
    total_ops: u64,
    throughput: f64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    error_rate: f64,
    success_count: u64,
    error_count: u64,
}

struct CsvRow {
    concurrent_clients: usize,
    elapsed_secs: f64,
    total_ops: u64,
    throughput: f64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    error_rate: f64,
    success_count: u64,
    error_count: u64,
}

async fn create_inventory_config(
    client: &MinioClient,
    bucket: &BucketName,
    job_id: &str,
    dest_bucket: &str,
    schedule: Schedule,
    format: OutputFormat,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.to_string(),
            prefix: Some(format!("{}/", job_id)),
            format,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    let _: PutInventoryConfigResponse = client
        .put_inventory_config(bucket.clone(), job_id, job)?
        .build()
        .send()
        .await?;

    Ok(())
}

async fn writer_task(
    client: MinioClient,
    bucket: BucketName,
    dest_bucket: String,
    config_ids: Vec<String>,
    metrics: Arc<MetricsCollector>,
    stop_signal: Arc<std::sync::atomic::AtomicBool>,
) {
    while !stop_signal.load(Ordering::Relaxed) {
        let config_idx = rand::rng().random_range(0..config_ids.len());
        let job_id = &config_ids[config_idx];

        let operation = match rand::rng().random_range(0..4) {
            0 => Operation::UpdateConfig,
            1 => Operation::ReadConfig,
            2 => Operation::ListConfigs,
            _ => Operation::DeleteAndRecreate,
        };

        let start = Instant::now();
        let success = match operation {
            Operation::UpdateConfig => {
                let schedule = match rand::rng().random_range(0..3) {
                    0 => Schedule::Daily,
                    1 => Schedule::Weekly,
                    _ => Schedule::Once,
                };

                let format = match rand::rng().random_range(0..2) {
                    0 => OutputFormat::CSV,
                    _ => OutputFormat::Parquet,
                };

                create_inventory_config(&client, &bucket, job_id, &dest_bucket, schedule, format)
                    .await
                    .is_ok()
            }

            Operation::ReadConfig => {
                let builder = match client.get_inventory_config(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(resp) => match resp.inventory_config() {
                        Ok(_) => true,
                        Err(e) => {
                            let err_msg = format!("{:?}", e);
                            let is_slow_down = err_msg.contains("SlowDown");
                            if !is_slow_down
                                && !err_msg.contains("PreconditionFailed")
                                && !err_msg.contains("NoSuchKey")
                                && !err_msg.contains("NoSuchInventoryConfiguration")
                            {
                                metrics.capture_unexpected_error(ErrorDetail {
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                    operation: "get_inventory_config".to_string(),
                                    bucket: bucket.to_string(),
                                    job_id: job_id.to_string(),
                                    error_type: "ParseError".to_string(),
                                    error_message: err_msg,
                                    latency_ms: start.elapsed().as_millis() as u64,
                                });
                            }
                            is_slow_down
                        }
                    },
                    Err(e) => {
                        let err_msg = format!("{:?}", e);
                        if err_msg.contains("SlowDown") {
                            true
                        } else if !err_msg.contains("PreconditionFailed")
                            && !err_msg.contains("NoSuchKey")
                            && !err_msg.contains("NoSuchInventoryConfiguration")
                        {
                            metrics.capture_unexpected_error(ErrorDetail {
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                operation: "get_inventory_config".to_string(),
                                bucket: bucket.to_string(),
                                job_id: job_id.to_string(),
                                error_type: "HttpError".to_string(),
                                error_message: err_msg,
                                latency_ms: start.elapsed().as_millis() as u64,
                            });
                            false
                        } else {
                            false
                        }
                    }
                }
            }

            Operation::ListConfigs => {
                let builder = match client.list_inventory_configs(bucket.clone()) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(resp) => match resp.configs() {
                        Ok(_) => true,
                        Err(e) => {
                            let err_msg = format!("{:?}", e);
                            let is_slow_down = err_msg.contains("SlowDown");
                            if !is_slow_down
                                && !err_msg.contains("PreconditionFailed")
                                && !err_msg.contains("NoSuchInventoryConfiguration")
                            {
                                metrics.capture_unexpected_error(ErrorDetail {
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                    operation: "list_inventory_configs".to_string(),
                                    bucket: bucket.to_string(),
                                    job_id: job_id.to_string(),
                                    error_type: "ParseError".to_string(),
                                    error_message: err_msg,
                                    latency_ms: start.elapsed().as_millis() as u64,
                                });
                            }
                            is_slow_down
                        }
                    },
                    Err(e) => {
                        let err_msg = format!("{:?}", e);
                        if err_msg.contains("SlowDown") {
                            true
                        } else if !err_msg.contains("PreconditionFailed")
                            && !err_msg.contains("NoSuchInventoryConfiguration")
                        {
                            metrics.capture_unexpected_error(ErrorDetail {
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                operation: "list_inventory_configs".to_string(),
                                bucket: bucket.to_string(),
                                job_id: job_id.to_string(),
                                error_type: "HttpError".to_string(),
                                error_message: err_msg,
                                latency_ms: start.elapsed().as_millis() as u64,
                            });
                            false
                        } else {
                            false
                        }
                    }
                }
            }

            Operation::DeleteAndRecreate => {
                let delete_ok = match client.delete_inventory_config(bucket.clone(), job_id) {
                    Ok(builder) => builder.build().send().await.is_ok(),
                    Err(_) => false,
                };

                if delete_ok {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    create_inventory_config(
                        &client,
                        &bucket,
                        job_id,
                        &dest_bucket,
                        Schedule::Daily,
                        OutputFormat::CSV,
                    )
                    .await
                    .is_ok()
                } else {
                    false
                }
            }
        };

        let latency_ms = start.elapsed().as_millis() as u64;
        metrics.record_operation(latency_ms, success);

        let delay_ms = rand::rng().random_range(10..50);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

struct LoadLevelParams<'a> {
    config: &'a TestConfig,
    base_url: &'a BaseUrl,
    static_provider: &'a StaticProvider,
    bucket: &'a BucketName,
    dest_bucket: &'a str,
    config_ids: &'a [String],
    test_start: Instant,
}

async fn run_load_level(
    params: &LoadLevelParams<'_>,
    concurrent_clients: usize,
) -> Result<(CsvRow, Vec<ErrorDetail>), Box<dyn std::error::Error + Send + Sync>> {
    let level_start = Instant::now();
    println!(
        "\n[{:>6.1}s] Starting load level: {} concurrent writers",
        params.test_start.elapsed().as_secs_f64(),
        concurrent_clients
    );

    let metrics = Arc::new(MetricsCollector::new());
    let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for _ in 0..concurrent_clients {
        let client = MinioClient::new(
            params.base_url.clone(),
            Some(params.static_provider.clone()),
            None,
            None,
        )?;
        let bucket_clone = params.bucket.clone();
        let dest_bucket_clone = params.dest_bucket.to_string();
        let config_ids_clone = params.config_ids.to_vec();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            writer_task(
                client,
                bucket_clone,
                dest_bucket_clone,
                config_ids_clone,
                metrics_clone,
                stop_signal_clone,
            )
            .await;
        });
    }

    tokio::time::sleep(Duration::from_secs(params.config.level_duration_secs)).await;

    stop_signal.store(true, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Task panicked: {}", e);
        }
    }

    let stats = metrics.compute_stats(level_start);
    let unexpected_errors = metrics.get_unexpected_errors();

    println!(
        "[{:>6.1}s] Completed: {:.1} ops/sec | P99: {}ms | Errors: {:.2}% | Unexpected: {}",
        params.test_start.elapsed().as_secs_f64(),
        stats.throughput,
        stats.latency_p99_ms,
        stats.error_rate * 100.0,
        unexpected_errors.len()
    );

    Ok((
        CsvRow {
            concurrent_clients,
            elapsed_secs: params.test_start.elapsed().as_secs_f64(),
            total_ops: stats.total_ops,
            throughput: stats.throughput,
            latency_mean_ms: stats.latency_mean_ms,
            latency_p50_ms: stats.latency_p50_ms,
            latency_p95_ms: stats.latency_p95_ms,
            latency_p99_ms: stats.latency_p99_ms,
            error_rate: stats.error_rate,
            success_count: stats.success_count,
            error_count: stats.error_count,
        },
        unexpected_errors,
    ))
}

fn write_csv_output(csv_data: &[CsvRow], filename: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(filename)?;

    writeln!(
        file,
        "concurrent_clients,elapsed_secs,total_ops,throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,success_count,error_count"
    )?;

    for row in csv_data {
        writeln!(
            file,
            "{},{:.2},{},{:.2},{:.2},{},{},{},{:.4},{},{}",
            row.concurrent_clients,
            row.elapsed_secs,
            row.total_ops,
            row.throughput,
            row.latency_mean_ms,
            row.latency_p50_ms,
            row.latency_p95_ms,
            row.latency_p99_ms,
            row.error_rate,
            row.success_count,
            row.error_count
        )?;
    }

    println!("\n[OK] Results written to: {}", filename);
    Ok(())
}

fn write_error_details(errors: &[ErrorDetail], filename: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(filename)?;

    writeln!(file, "=== WRITE CONTENTION ERROR ANALYSIS ===")?;
    writeln!(file, "Test Date: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(file, "Total Unexpected Errors Captured: {}", errors.len())?;
    writeln!(file)?;
    writeln!(file, "=== UNEXPECTED ERROR DETAILS (Last 10) ===")?;
    writeln!(file)?;

    if errors.is_empty() {
        writeln!(file, "No unexpected errors detected.")?;
        writeln!(file)?;
        writeln!(file, "Expected error types (filtered, not captured):")?;
        writeln!(
            file,
            "  - Write conflicts (PreconditionFailed - 409/412 status codes)"
        )?;
        writeln!(
            file,
            "  - Race conditions (NoSuchInventoryConfiguration from delete/recreate)"
        )?;
        writeln!(file, "  - Rate limiting (SlowDown - server backpressure)")?;
    } else {
        for (i, error) in errors.iter().enumerate() {
            writeln!(file, "Error #{}", i + 1)?;
            writeln!(file, "  Timestamp:    {}", error.timestamp)?;
            writeln!(file, "  Operation:    {}", error.operation)?;
            writeln!(file, "  Bucket:       {}", error.bucket)?;
            writeln!(file, "  Job ID:       {}", error.job_id)?;
            writeln!(file, "  Error Type:   {}", error.error_type)?;
            writeln!(file, "  Latency:      {}ms", error.latency_ms)?;
            writeln!(file, "  Error Message:")?;
            writeln!(file, "    {}", error.error_message)?;
            writeln!(file)?;
        }
    }

    println!("[OK] Error details written to: {}", filename);
    Ok(())
}

fn parse_args() -> TestConfig {
    let args: Vec<String> = std::env::args().collect();
    let mut config = TestConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--start-clients" => {
                config.start_clients = args[i + 1].parse().unwrap_or(5);
                i += 2;
            }
            "--max-clients" => {
                config.max_clients = args[i + 1].parse().unwrap_or(50);
                i += 2;
            }
            "--step" => {
                config.step = args[i + 1].parse().unwrap_or(5);
                i += 2;
            }
            "--level-duration" => {
                config.level_duration_secs = args[i + 1].parse().unwrap_or(30);
                i += 2;
            }
            "--num-configs" => {
                config.num_configs = args[i + 1].parse().unwrap_or(3);
                i += 2;
            }
            _ => i += 1,
        }
    }

    config
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args();

    println!("=== S3 Inventory Stress Test: Write Contention Saturation Analysis ===\n");
    println!("Configuration:");
    println!("  Start clients:    {}", config.start_clients);
    println!("  Max clients:      {}", config.max_clients);
    println!("  Step size:        {}", config.step);
    println!("  Level duration:   {}s", config.level_duration_secs);
    println!("  Shared configs:   {}", config.num_configs);

    let num_levels = (config.max_clients - config.start_clients) / config.step + 1;
    let total_duration = num_levels * config.level_duration_secs as usize;
    println!("  Total levels:     {}", num_levels);
    println!(
        "  Estimated time:   ~{}s ({:.1} min)\n",
        total_duration,
        total_duration as f64 / 60.0
    );

    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;

    let bucket = BucketName::new("write-contention-bucket").unwrap();
    let dest_bucket = "write-contention-reports";

    println!("Step 1: Creating test buckets...");
    let _ = client.create_bucket(bucket.clone()).unwrap().build().send().await;
    let _ = client
        .create_bucket(BucketName::new(dest_bucket).unwrap())
        .unwrap()
        .build()
        .send()
        .await;
    println!("  Buckets ready");

    println!(
        "\nStep 2: Creating {} shared inventory configs (write targets)...",
        config.num_configs
    );
    let mut config_ids = Vec::new();
    for i in 0..config.num_configs {
        let job_id = format!("config-{}", i);
        create_inventory_config(
            &client,
            &bucket,
            &job_id,
            dest_bucket,
            Schedule::Daily,
            OutputFormat::CSV,
        )
        .await?;
        config_ids.push(job_id.clone());
        println!("  Created config '{}'", job_id);
    }

    println!("\nStep 3: Running saturation test with incremental load...");
    let test_start = Instant::now();
    let mut csv_data = Vec::new();
    let mut all_unexpected_errors = Vec::new();

    let params = LoadLevelParams {
        config: &config,
        base_url: &base_url,
        static_provider: &static_provider,
        bucket: &bucket,
        dest_bucket,
        config_ids: &config_ids,
        test_start,
    };

    let mut current_clients = config.start_clients;
    while current_clients <= config.max_clients {
        let (row, errors) = run_load_level(&params, current_clients).await?;

        csv_data.push(row);
        all_unexpected_errors.extend(errors);
        if all_unexpected_errors.len() > 10 {
            all_unexpected_errors.drain(0..all_unexpected_errors.len() - 10);
        }
        current_clients += config.step;
    }

    println!("\n=== Saturation Test Completed ===");
    println!("Total duration: {:.1}s", test_start.elapsed().as_secs_f64());

    write_csv_output(&csv_data, "write_contention_saturation.csv")?;
    write_error_details(&all_unexpected_errors, "write_contention_errors.txt")?;

    println!("\n[NEXT] Visualize results:");
    println!("  python examples/s3inventory/plot_write_contention.py");

    Ok(())
}
