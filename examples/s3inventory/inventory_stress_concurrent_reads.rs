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

//! Stress test: Read scalability with saturation analysis.
//!
//! This stress test evaluates read throughput scalability by gradually increasing
//! the number of concurrent readers while keeping a small number of writers constant.
//! It measures read throughput saturation, latency degradation, and reader/writer
//! isolation under increasing read load.
//!
//! # Test Scenario
//!
//! 1. Create multiple inventory configurations
//! 2. Gradually increase concurrent readers from START_READERS to MAX_READERS
//! 3. Keep writer count constant (minimal write interference)
//! 4. Each load level runs for LEVEL_DURATION_SECS seconds
//! 5. Readers perform: ListConfigs, GetConfig, CheckJobStatus
//! 6. Writers perform: UpdateConfig, PutObjects (occasional)
//! 7. Collect read throughput, latency percentiles (P50/P95/P99), and error rates
//! 8. Generate CSV output for saturation analysis visualization
//!
//! # Configuration (CLI Arguments)
//!
//! - `--start-readers`: Initial concurrent readers (default: 10)
//! - `--max-readers`: Maximum concurrent readers (default: 100)
//! - `--step`: Reader increment per level (default: 10)
//! - `--level-duration`: Seconds per load level (default: 30)
//! - `--num-configs`: Inventory configs to create (default: 5)
//! - `--num-writers`: Constant writer count (default: 3)
//!
//! # Expected Behavior
//!
//! - Read throughput increases linearly initially
//! - Eventually plateaus/saturates due to server/network limits
//! - Read latency increases as concurrency grows
//! - Writers should not significantly impact reader performance
//! - Demonstrates read scalability characteristics
//!
//! # Requirements
//!
//! - MinIO server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin

use minio::s3::MinioClient;
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use minio::s3inventory::{
    DestinationSpec, JobDefinition, ModeSpec, OnOrOff, OutputFormat, PutInventoryConfigResponse,
    Schedule, VersionsSpec,
};
use rand::Rng;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Clone)]
struct TestConfig {
    start_readers: usize,
    max_readers: usize,
    step: usize,
    level_duration_secs: u64,
    num_configs: usize,
    num_writers: usize,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            start_readers: 10,
            max_readers: 100,
            step: 10,
            level_duration_secs: 30,
            num_configs: 5,
            num_writers: 3,
        }
    }
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
    read_operations: Arc<Mutex<Vec<OperationMetric>>>,
    write_operations: Arc<Mutex<Vec<OperationMetric>>>,
    read_counter: AtomicU64,
    read_error_counter: AtomicU64,
    write_counter: AtomicU64,
    write_error_counter: AtomicU64,
    error_details: Arc<Mutex<Vec<ErrorDetail>>>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            read_operations: Arc::new(Mutex::new(Vec::new())),
            write_operations: Arc::new(Mutex::new(Vec::new())),
            read_counter: AtomicU64::new(0),
            read_error_counter: AtomicU64::new(0),
            write_counter: AtomicU64::new(0),
            write_error_counter: AtomicU64::new(0),
            error_details: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record_read(&self, latency_ms: u64, success: bool) {
        if success {
            self.read_counter.fetch_add(1, Ordering::Relaxed);
        } else {
            self.read_error_counter.fetch_add(1, Ordering::Relaxed);
        }

        let mut ops = self.read_operations.lock().unwrap();
        ops.push(OperationMetric {
            latency_ms,
            success,
            _timestamp: Instant::now(),
        });
    }

    fn record_write(&self, latency_ms: u64, success: bool) {
        if success {
            self.write_counter.fetch_add(1, Ordering::Relaxed);
        } else {
            self.write_error_counter.fetch_add(1, Ordering::Relaxed);
        }

        let mut ops = self.write_operations.lock().unwrap();
        ops.push(OperationMetric {
            latency_ms,
            success,
            _timestamp: Instant::now(),
        });
    }

    fn capture_error(&self, detail: ErrorDetail) {
        let mut errors = self.error_details.lock().unwrap();
        if errors.len() < 10 {
            errors.push(detail);
        }
    }

    fn get_error_count(&self) -> usize {
        self.error_details.lock().unwrap().len()
    }

    fn get_error_details(&self) -> Vec<ErrorDetail> {
        self.error_details.lock().unwrap().clone()
    }

    fn compute_read_stats(&self, test_start: Instant) -> AggregateStats {
        let ops = self.read_operations.lock().unwrap();
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
    concurrent_readers: usize,
    elapsed_secs: f64,
    total_read_ops: u64,
    read_throughput: f64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    read_error_rate: f64,
    read_success_count: u64,
    read_error_count: u64,
}

async fn reader_task(
    client: MinioClient,
    config_info: Vec<(BucketName, String)>,
    metrics: Arc<MetricsCollector>,
    stop_signal: Arc<AtomicBool>,
) {
    while !stop_signal.load(Ordering::Relaxed) {
        let operation = rand::rng().random_range(0..3);

        let start = Instant::now();
        let success = match operation {
            0 => {
                let config_idx = rand::rng().random_range(0..config_info.len());
                let (bucket, _) = &config_info[config_idx];

                let builder = match client.list_inventory_configs(bucket.clone()) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(resp) => match resp.configs() {
                        Ok(_) => true,
                        Err(e) => {
                            if metrics.get_error_count() < 10 {
                                metrics.capture_error(ErrorDetail {
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                    operation: "list_inventory_configs".to_string(),
                                    bucket: bucket.to_string(),
                                    job_id: "N/A".to_string(),
                                    error_type: "ParseError".to_string(),
                                    error_message: format!("{:?}", e),
                                    latency_ms: start.elapsed().as_millis() as u64,
                                });
                            }
                            false
                        }
                    },
                    Err(e) => {
                        if metrics.get_error_count() < 10 {
                            metrics.capture_error(ErrorDetail {
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                operation: "list_inventory_configs".to_string(),
                                bucket: bucket.to_string(),
                                job_id: "N/A".to_string(),
                                error_type: "HttpError".to_string(),
                                error_message: format!("{:?}", e),
                                latency_ms: start.elapsed().as_millis() as u64,
                            });
                        }
                        false
                    }
                }
            }

            1 => {
                let config_idx = rand::rng().random_range(0..config_info.len());
                let (bucket, job_id) = &config_info[config_idx];

                let builder = match client.get_inventory_config(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(resp) => match resp.inventory_config() {
                        Ok(_) => true,
                        Err(e) => {
                            if metrics.get_error_count() < 10 {
                                metrics.capture_error(ErrorDetail {
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                    operation: "get_inventory_config".to_string(),
                                    bucket: bucket.to_string(),
                                    job_id: job_id.to_string(),
                                    error_type: "ParseError".to_string(),
                                    error_message: format!("{:?}", e),
                                    latency_ms: start.elapsed().as_millis() as u64,
                                });
                            }
                            false
                        }
                    },
                    Err(e) => {
                        if metrics.get_error_count() < 10 {
                            metrics.capture_error(ErrorDetail {
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                operation: "get_inventory_config".to_string(),
                                bucket: bucket.to_string(),
                                job_id: job_id.to_string(),
                                error_type: "HttpError".to_string(),
                                error_message: format!("{:?}", e),
                                latency_ms: start.elapsed().as_millis() as u64,
                            });
                        }
                        false
                    }
                }
            }

            _ => {
                let config_idx = rand::rng().random_range(0..config_info.len());
                let (bucket, job_id) = &config_info[config_idx];

                let builder = match client.get_inventory_job_status(bucket.clone(), job_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match builder.build().send().await {
                    Ok(resp) => match resp.status() {
                        Ok(_) => true,
                        Err(e) => {
                            if metrics.get_error_count() < 10 {
                                metrics.capture_error(ErrorDetail {
                                    timestamp: chrono::Utc::now().to_rfc3339(),
                                    operation: "get_inventory_job_status".to_string(),
                                    bucket: bucket.to_string(),
                                    job_id: job_id.to_string(),
                                    error_type: "ParseError".to_string(),
                                    error_message: format!("{:?}", e),
                                    latency_ms: start.elapsed().as_millis() as u64,
                                });
                            }
                            false
                        }
                    },
                    Err(e) => {
                        if metrics.get_error_count() < 10 {
                            metrics.capture_error(ErrorDetail {
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                operation: "get_inventory_job_status".to_string(),
                                bucket: bucket.to_string(),
                                job_id: job_id.to_string(),
                                error_type: "HttpError".to_string(),
                                error_message: format!("{:?}", e),
                                latency_ms: start.elapsed().as_millis() as u64,
                            });
                        }
                        false
                    }
                }
            }
        };

        let latency_ms = start.elapsed().as_millis() as u64;
        metrics.record_read(latency_ms, success);

        let delay_ms = rand::rng().random_range(10..50);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

async fn writer_task(
    client: MinioClient,
    config_info: Vec<(BucketName, String)>,
    dest_bucket: BucketName,
    metrics: Arc<MetricsCollector>,
    stop_signal: Arc<AtomicBool>,
) {
    let mut op_count = 0;

    while !stop_signal.load(Ordering::Relaxed) {
        let config_idx = rand::rng().random_range(0..config_info.len());
        let (bucket, job_id) = &config_info[config_idx];

        let write_type = rand::rng().random_range(0..2);

        let start = Instant::now();
        let success = match write_type {
            0 => {
                let object_name = format!("concurrent-write-{}.dat", op_count);
                let content = vec![b'D'; 512];
                let object_content = ObjectContent::from(content);

                client
                    .put_object_content(
                        bucket.clone(),
                        ObjectKey::new(&object_name).unwrap(),
                        object_content,
                    )
                    .unwrap()
                    .build()
                    .send()
                    .await
                    .is_ok()
            }

            _ => {
                let schedule = match rand::rng().random_range(0..2) {
                    0 => Schedule::Daily,
                    _ => Schedule::Weekly,
                };

                let job = JobDefinition {
                    api_version: "v1".to_string(),
                    id: job_id.to_string(),
                    destination: DestinationSpec {
                        bucket: dest_bucket.to_string(),
                        prefix: Some(format!("{}/", job_id)),
                        format: OutputFormat::CSV,
                        compression: OnOrOff::On,
                        max_file_size_hint: None,
                    },
                    schedule,
                    mode: ModeSpec::Fast,
                    versions: VersionsSpec::Current,
                    include_fields: vec![],
                    filters: None,
                };

                match client.put_inventory_config(bucket.clone(), job_id, job) {
                    Ok(builder) => builder.build().send().await.is_ok(),
                    Err(_) => false,
                }
            }
        };

        let latency_ms = start.elapsed().as_millis() as u64;
        metrics.record_write(latency_ms, success);

        op_count += 1;

        let delay_ms = rand::rng().random_range(100..500);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

async fn run_load_level(
    config: &TestConfig,
    concurrent_readers: usize,
    base_url: &BaseUrl,
    static_provider: &StaticProvider,
    config_info: &[(BucketName, String)],
    dest_bucket: &BucketName,
    test_start: Instant,
) -> Result<(CsvRow, Vec<ErrorDetail>), Box<dyn std::error::Error + Send + Sync>> {
    let level_start = Instant::now();
    println!(
        "\n[{:>6.1}s] Starting load level: {} readers + {} writers",
        test_start.elapsed().as_secs_f64(),
        concurrent_readers,
        config.num_writers
    );

    let metrics = Arc::new(MetricsCollector::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for _ in 0..concurrent_readers {
        let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;
        let config_info_clone = config_info.to_vec();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            reader_task(client, config_info_clone, metrics_clone, stop_signal_clone).await;
        });
    }

    for _ in 0..config.num_writers {
        let client = MinioClient::new(base_url.clone(), Some(static_provider.clone()), None, None)?;
        let config_info_clone = config_info.to_vec();
        let dest_bucket_clone = dest_bucket.clone();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            writer_task(
                client,
                config_info_clone,
                dest_bucket_clone,
                metrics_clone,
                stop_signal_clone,
            )
            .await;
        });
    }

    tokio::time::sleep(Duration::from_secs(config.level_duration_secs)).await;

    stop_signal.store(true, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Task panicked: {}", e);
        }
    }

    let stats = metrics.compute_read_stats(level_start);

    let error_details = metrics.get_error_details();
    let errors_captured = error_details.len();

    println!(
        "[{:>6.1}s] Completed: {:.1} read ops/sec | P99: {}ms | Read errors: {:.2}% | Captured: {}",
        test_start.elapsed().as_secs_f64(),
        stats.throughput,
        stats.latency_p99_ms,
        stats.error_rate * 100.0,
        errors_captured
    );

    Ok((
        CsvRow {
            concurrent_readers,
            elapsed_secs: test_start.elapsed().as_secs_f64(),
            total_read_ops: stats.total_ops,
            read_throughput: stats.throughput,
            latency_mean_ms: stats.latency_mean_ms,
            latency_p50_ms: stats.latency_p50_ms,
            latency_p95_ms: stats.latency_p95_ms,
            latency_p99_ms: stats.latency_p99_ms,
            read_error_rate: stats.error_rate,
            read_success_count: stats.success_count,
            read_error_count: stats.error_count,
        },
        error_details,
    ))
}

fn write_csv_output(csv_data: &[CsvRow], filename: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(filename)?;

    writeln!(
        file,
        "concurrent_readers,elapsed_secs,total_read_ops,read_throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,read_error_rate,read_success_count,read_error_count"
    )?;

    for row in csv_data {
        writeln!(
            file,
            "{},{:.2},{},{:.2},{:.2},{},{},{},{:.4},{},{}",
            row.concurrent_readers,
            row.elapsed_secs,
            row.total_read_ops,
            row.read_throughput,
            row.latency_mean_ms,
            row.latency_p50_ms,
            row.latency_p95_ms,
            row.latency_p99_ms,
            row.read_error_rate,
            row.read_success_count,
            row.read_error_count
        )?;
    }

    println!("\n[OK] Results written to: {}", filename);
    Ok(())
}

fn write_error_details(errors: &[ErrorDetail], filename: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(filename)?;

    writeln!(file, "=== READ ERROR ANALYSIS ===")?;
    writeln!(file, "Test Date: {}", chrono::Utc::now().to_rfc3339())?;
    writeln!(file, "Total Errors Captured: {}", errors.len())?;
    writeln!(file)?;
    writeln!(file, "=== ERROR DETAILS (First 10) ===")?;
    writeln!(file)?;

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

    println!("[OK] Error details written to: {}", filename);
    Ok(())
}

fn parse_args() -> TestConfig {
    let args: Vec<String> = std::env::args().collect();
    let mut config = TestConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--start-readers" => {
                config.start_readers = args[i + 1].parse().unwrap_or(10);
                i += 2;
            }
            "--max-readers" => {
                config.max_readers = args[i + 1].parse().unwrap_or(100);
                i += 2;
            }
            "--step" => {
                config.step = args[i + 1].parse().unwrap_or(10);
                i += 2;
            }
            "--level-duration" => {
                config.level_duration_secs = args[i + 1].parse().unwrap_or(30);
                i += 2;
            }
            "--num-configs" => {
                config.num_configs = args[i + 1].parse().unwrap_or(5);
                i += 2;
            }
            "--num-writers" => {
                config.num_writers = args[i + 1].parse().unwrap_or(3);
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

    println!("=== S3 Inventory Stress Test: Read Scalability Saturation Analysis ===\n");
    println!("Configuration:");
    println!("  Start readers:    {}", config.start_readers);
    println!("  Max readers:      {}", config.max_readers);
    println!("  Step size:        {}", config.step);
    println!("  Level duration:   {}s", config.level_duration_secs);
    println!("  Inventory configs:{}", config.num_configs);
    println!("  Constant writers: {}", config.num_writers);

    let num_levels = (config.max_readers - config.start_readers) / config.step + 1;
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

    let dest_bucket = BucketName::new("read-scalability-reports").unwrap();
    println!("Step 1: Creating destination bucket...");
    let _ = client
        .create_bucket(dest_bucket.clone())
        .unwrap()
        .build()
        .send()
        .await;
    println!("  Bucket ready");

    let mut config_info = Vec::new();

    println!(
        "\nStep 2: Creating {} inventory configurations...",
        config.num_configs
    );
    for i in 0..config.num_configs {
        let bucket = BucketName::new(format!("read-scalability-{}", i)).unwrap();
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

        config_info.push((bucket.clone(), job_id.clone()));
        println!("  Created config '{}' on bucket '{}'", job_id, bucket);
    }

    println!("\nStep 3: Running saturation test with incremental read load...");
    let test_start = Instant::now();
    let mut csv_data = Vec::new();
    let mut all_errors = Vec::new();

    let mut current_readers = config.start_readers;
    while current_readers <= config.max_readers {
        let (row, errors) = run_load_level(
            &config,
            current_readers,
            &base_url,
            &static_provider,
            &config_info,
            &dest_bucket,
            test_start,
        )
        .await?;

        csv_data.push(row);
        all_errors.extend(errors);
        current_readers += config.step;
    }

    println!("\n=== Saturation Test Completed ===");
    println!("Total duration: {:.1}s", test_start.elapsed().as_secs_f64());

    write_csv_output(&csv_data, "read_scalability_saturation.csv")?;
    write_error_details(&all_errors, "read_saturation_errors.txt")?;

    println!("\n[NEXT] Visualize results:");
    println!("  python examples/s3inventory/plot_read_scalability.py");

    Ok(())
}
