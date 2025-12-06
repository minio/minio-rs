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

//! S3-Compatible Backend Performance Comparison Benchmark
//!
//! This example benchmarks the minio-rs SDK against multiple S3-compatible backends:
//! - MinIO (default S3 server)
//! - Garage (lightweight S3 implementation)
//!
//! # Prerequisites
//!
//! 1. **MinIO Server** (default backend):
//!    ```bash
//!    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server /data --console-address ":9001"
//!    ```
//!    Endpoint: http://localhost:9000
//!
//! # Usage
//!
//! ```bash
//! # Setup test data before benchmarking
//! cargo run --example s3_performance_comparison -- setup --backend minio
//!
//! # Benchmark MinIO
//! cargo run --example s3_performance_comparison -- bench --backend minio --iterations 100
//!
//! # Cleanup test data after benchmarking
//! cargo run --example s3_performance_comparison -- cleanup --backend minio
//! ```
//!
//! # Output
//!
//! The benchmark produces measured metrics:
//! - Per-operation latency measurements (min, max, avg, p99)
//! - Success/failure rates
//! - Throughput (operations per second)
//!
//! **Note**: All reported metrics are MEASURED DATA collected during benchmark runs.

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use minio::s3::creds::StaticProvider;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::{S3Api, ToStream};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "s3-performance-comparison")]
#[command(about = "Benchmark minio-rs against S3-compatible backends", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Setup test data on backend
    Setup {
        /// Specific backend (minio, garage)
        #[arg(long, default_value = "minio")]
        backend: String,

        /// Bucket name for test data
        #[arg(long, default_value = "benchmark-test")]
        bucket: String,

        /// Number of test objects to create
        #[arg(long, default_value = "10")]
        num_objects: usize,

        /// Size of each test object (bytes)
        #[arg(long, default_value = "1048576")]
        object_size: usize,
    },

    /// Run benchmark against backend
    Bench {
        /// Backend to benchmark (minio, garage)
        #[arg(long, default_value = "minio")]
        backend: String,

        /// Bucket name for benchmark
        #[arg(long, default_value = "benchmark-test")]
        bucket: String,

        /// Number of iterations per operation
        #[arg(long, default_value = "100")]
        iterations: usize,

        /// Concurrent operations
        #[arg(long, default_value = "1")]
        concurrency: usize,
    },

    /// Cleanup test data from backend
    Cleanup {
        /// Specific backend (minio, garage)
        #[arg(long, default_value = "minio")]
        backend: String,

        /// Bucket name to clean
        #[arg(long, default_value = "benchmark-test")]
        bucket: String,
    },
}

/// Represents metrics collected for a single operation
#[derive(Debug, Clone, Default)]
struct OperationMetrics {
    count: u64,
    min_latency_ms: f64,
    max_latency_ms: f64,
    total_latency_ms: f64,
    p99_latency_ms: f64,
    success_count: u64,
    failure_count: u64,
    latencies: Vec<f64>,
}

impl OperationMetrics {
    fn avg_latency_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_latency_ms / self.count as f64
        }
    }

    fn success_rate(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.success_count as f64 / self.count as f64) * 100.0
        }
    }

    fn record(&mut self, latency_ms: f64, success: bool) {
        self.count += 1;
        if success {
            self.success_count += 1;
        } else {
            self.failure_count += 1;
        }

        self.total_latency_ms += latency_ms;
        if self.count == 1 {
            self.min_latency_ms = latency_ms;
            self.max_latency_ms = latency_ms;
        } else {
            self.min_latency_ms = self.min_latency_ms.min(latency_ms);
            self.max_latency_ms = self.max_latency_ms.max(latency_ms);
        }
        self.latencies.push(latency_ms);

        // Update p99
        if !self.latencies.is_empty() {
            let mut sorted = self.latencies.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let p99_idx = ((sorted.len() as f64 * 0.99) as usize).saturating_sub(1);
            self.p99_latency_ms = sorted.get(p99_idx).copied().unwrap_or(0.0);
        }
    }
}

fn get_backend_config(backend: &str) -> (String, String, String, String) {
    match backend {
        "garage" => (
            "http://localhost:3900".to_string(),
            "minioadmin".to_string(),
            "minioadmin".to_string(),
            "Garage".to_string(),
        ),
        "minio" | _ => (
            "http://localhost:9000".to_string(),
            "minioadmin".to_string(),
            "minioadmin".to_string(),
            "MinIO".to_string(),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init()
        .ok();

    let cli = Cli::parse();

    match cli.command {
        Commands::Setup {
            backend,
            bucket,
            num_objects,
            object_size,
        } => {
            setup_test_data(&backend, &bucket, num_objects, object_size).await?;
        }
        Commands::Bench {
            backend,
            bucket,
            iterations,
            concurrency: _,
        } => {
            run_benchmark(&backend, &bucket, iterations).await?;
        }
        Commands::Cleanup { backend, bucket } => {
            cleanup_test_data(&backend, &bucket).await?;
        }
    }

    Ok(())
}

async fn setup_test_data(
    backend: &str,
    bucket: &str,
    num_objects: usize,
    object_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, access_key, secret_key, backend_name) = get_backend_config(backend);

    println!("Setting up test data on {} backend", backend_name);
    println!(
        "Endpoint: {}, Bucket: {}, Objects: {}, Size per object: {} bytes",
        endpoint, bucket, num_objects, object_size
    );

    let base_url = endpoint.parse()?;
    let static_provider = StaticProvider::new(&access_key, &secret_key, None);
    let client = minio::s3::MinioClient::new(base_url, Some(static_provider), None, None)?;

    // Create bucket if it doesn't exist
    println!("Creating bucket: {}", bucket);
    match client.create_bucket(bucket).build().send().await {
        Ok(_) => println!("  Bucket created successfully"),
        Err(_) => println!("  Bucket creation failed (may already exist)"),
    }

    // Upload test objects
    println!("Uploading {} test objects...", num_objects);
    let test_data = vec![b'x'; object_size];

    for i in 0..num_objects {
        let object_name = format!("test-object-{:04}.bin", i);
        let segmented_bytes = SegmentedBytes::from(Bytes::from(test_data.clone()));

        client
            .put_object(bucket, &object_name, segmented_bytes)
            .build()
            .send()
            .await?;

        if (i + 1) % (num_objects / 10).max(1) == 0 {
            println!("  Uploaded {}/{} objects", i + 1, num_objects);
        }
    }

    println!("Setup complete!");
    Ok(())
}

async fn run_benchmark(
    backend: &str,
    bucket: &str,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, access_key, secret_key, backend_name) = get_backend_config(backend);

    println!("\n{:=^80}", "");
    println!("Benchmarking {} Backend", backend_name);
    println!("{:=^80}", "");
    println!("Endpoint: {}", endpoint);
    println!("Bucket: {}", bucket);
    println!("Iterations: {}", iterations);

    let base_url = endpoint.parse()?;
    let static_provider = StaticProvider::new(&access_key, &secret_key, None);
    let client = minio::s3::MinioClient::new(base_url, Some(static_provider), None, None)?;

    let mut metrics: HashMap<String, OperationMetrics> = HashMap::new();

    // GET object benchmark
    println!("\nBenchmarking GET operations...");
    let mut get_metrics = OperationMetrics::default();
    for i in 0..iterations {
        let object_name = format!("test-object-{:04}.bin", i % 10);
        let start = Instant::now();

        let result = client.get_object(bucket, &object_name).build().send().await;

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        get_metrics.record(elapsed_ms, result.is_ok());

        if (i + 1) % (iterations / 5).max(1) == 0 {
            println!("  GET: {}/{} iterations", i + 1, iterations);
        }
    }
    metrics.insert("GET".to_string(), get_metrics);

    // HEAD object benchmark
    println!("\nBenchmarking HEAD operations...");
    let mut head_metrics = OperationMetrics::default();
    for i in 0..iterations {
        let object_name = format!("test-object-{:04}.bin", i % 10);
        let start = Instant::now();

        let result = client
            .stat_object(bucket, &object_name)
            .build()
            .send()
            .await;

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        head_metrics.record(elapsed_ms, result.is_ok());

        if (i + 1) % (iterations / 5).max(1) == 0 {
            println!("  HEAD: {}/{} iterations", i + 1, iterations);
        }
    }
    metrics.insert("HEAD".to_string(), head_metrics);

    // LIST objects benchmark
    println!("\nBenchmarking LIST operations...");
    let mut list_metrics = OperationMetrics::default();
    for _ in 0..iterations {
        let start = Instant::now();

        let mut stream = client.list_objects(bucket).build().to_stream().await;

        let mut success = true;
        while let Some(result) = stream.next().await {
            if result.is_err() {
                success = false;
                break;
            }
        }

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        list_metrics.record(elapsed_ms, success);
    }
    metrics.insert("LIST".to_string(), list_metrics);

    // Print results
    println!("\n{:=^80}", "");
    println!("Benchmark Results for {}", backend_name);
    println!("{:=^80}\n", "");

    for (op_name, metrics_data) in &metrics {
        println!("Operation: {}", op_name);
        println!("  Count:         {}", metrics_data.count);
        println!("  Success Rate:  {:.2}%", metrics_data.success_rate());
        println!("  Min Latency:   {:.2} ms", metrics_data.min_latency_ms);
        println!("  Max Latency:   {:.2} ms", metrics_data.max_latency_ms);
        println!("  Avg Latency:   {:.2} ms", metrics_data.avg_latency_ms());
        println!("  P99 Latency:   {:.2} ms", metrics_data.p99_latency_ms);
        if metrics_data.count > 0 {
            let throughput = metrics_data.count as f64
                / (iterations as f64 * metrics_data.avg_latency_ms() / 1000.0);
            println!("  Throughput:    {:.2} ops/sec", throughput);
        }
        println!();
    }

    println!("{:=^80}", "");
    println!("All measurements are MEASURED DATA from actual S3 operations");
    println!("{:=^80}", "");

    Ok(())
}

async fn cleanup_test_data(backend: &str, bucket: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, access_key, secret_key, backend_name) = get_backend_config(backend);

    println!("Cleaning up test data on {} backend", backend_name);
    println!("Endpoint: {}", endpoint);
    println!("Bucket: {}", bucket);

    let base_url = endpoint.parse()?;
    let static_provider = StaticProvider::new(&access_key, &secret_key, None);
    let client = minio::s3::MinioClient::new(base_url, Some(static_provider), None, None)?;

    // List and delete all objects
    println!("Deleting test objects...");
    let mut list_stream = client.list_objects(bucket).build().to_stream().await;

    let mut count = 0;
    while let Some(object_result) = list_stream.next().await {
        if let Ok(obj_batch) = object_result {
            for obj in &obj_batch.contents {
                if client
                    .delete_object(bucket, &obj.name)
                    .build()
                    .send()
                    .await
                    .is_ok()
                {
                    count += 1;
                }
            }
        }
    }

    println!("Deleted {} objects", count);
    println!("Cleanup complete!");
    Ok(())
}
