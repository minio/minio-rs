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

//! Storage Pushdown Benchmark for ExecuteTableScan
//!
//! This example benchmarks the performance difference between:
//! - Standard path: Read full Parquet files, then apply SIMD filter
//! - Storage pushdown: Apply SIMD filter during Parquet reading
//!
//! # What It Measures
//!
//! The benchmark sends table scan requests with ILIKE filters to MinIO AIStor
//! and measures the execution time for each mode:
//!
//! - **Standard**: Full Parquet read + post-read SIMD filtering
//! - **Storage Pushdown**: SIMD filtering during Parquet read (less I/O)
//!
//! # Prerequisites
//!
//! 1. MinIO AIStor running with S3 Tables support (with storage pushdown patches)
//! 2. A warehouse with a table containing string data for ILIKE filtering
//!
//! # Usage
//!
//! ```bash
//! cargo run --example storage_pushdown_benchmark
//! ```

use minio::s3tables::builders::OutputFormat;
use minio::s3tables::filter::FilterBuilder;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{Namespace, SimdMode, TableName, WarehouseName};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};
use std::env;
use std::time::{Duration, Instant};

const DEFAULT_ENDPOINT: &str = "http://localhost:9000";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";

const BENCHMARK_WAREHOUSE: &str = "simd-bench";
const BENCHMARK_NAMESPACE: &str = "data";
const BENCHMARK_TABLE: &str = "products";

#[derive(Debug, Clone, Copy)]
enum PushdownMode {
    Standard,
    StoragePushdown,
}

impl PushdownMode {
    fn as_str(&self) -> &'static str {
        match self {
            PushdownMode::Standard => "Standard",
            PushdownMode::StoragePushdown => "Storage Pushdown",
        }
    }
}

#[derive(Debug)]
struct BenchmarkResult {
    mode: PushdownMode,
    duration: Duration,
    rows_returned: usize,
    bytes_transferred: usize,
    scan_path: String,
}

impl BenchmarkResult {
    fn throughput_mbps(&self) -> f64 {
        let seconds: f64 = self.duration.as_secs_f64();
        if seconds > 0.0 {
            (self.bytes_transferred as f64) / (1024.0 * 1024.0 * seconds)
        } else {
            0.0
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("==============================================");
    println!("  STORAGE PUSHDOWN BENCHMARK");
    println!("  Standard vs Storage-Level Filtering");
    println!("==============================================\n");

    let endpoint: String =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
    let access_key: String =
        env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
    let secret_key: String =
        env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| DEFAULT_SECRET_KEY.to_string());

    println!("Configuration:");
    println!("  Endpoint: {}", endpoint);
    println!(
        "  Access Key: {}****",
        &access_key[..access_key.len().min(4)]
    );
    println!();

    let args: Vec<String> = env::args().collect();
    let warehouse_name: String =
        get_arg(&args, "--warehouse").unwrap_or_else(|| BENCHMARK_WAREHOUSE.to_string());
    let namespace_name: String =
        get_arg(&args, "--namespace").unwrap_or_else(|| BENCHMARK_NAMESPACE.to_string());
    let table_name: String =
        get_arg(&args, "--table").unwrap_or_else(|| BENCHMARK_TABLE.to_string());

    let client: TablesClient = TablesClient::builder()
        .endpoint(&endpoint)
        .credentials(&access_key, &secret_key)
        .build()?;

    let warehouse: WarehouseName = WarehouseName::try_from(warehouse_name.as_str())?;
    let namespace: Namespace = Namespace::single(&namespace_name)?;
    let table: TableName = TableName::new(&table_name)?;

    let table_exists: bool = check_table_exists(&client, &warehouse, &namespace, &table).await;

    if !table_exists {
        println!("Table not found. Creating test data...\n");
        setup_test_data(&client, &warehouse, &namespace, &table).await?;
    } else {
        println!(
            "Using existing table: {}/{}/{}\n",
            warehouse_name, namespace_name, table_name
        );
    }

    // Build ILIKE filter for benchmarking
    // Use "enterprise" which appears in ~1/8 of description templates
    let filter: minio::s3tables::filter::Filter =
        FilterBuilder::column("description").contains_i("enterprise");

    println!("Filter: description ILIKE '%enterprise%'");
    println!();

    // Run benchmarks
    println!("Running benchmarks (3 iterations each)...\n");

    let modes: [PushdownMode; 2] = [PushdownMode::Standard, PushdownMode::StoragePushdown];
    let mut results: Vec<BenchmarkResult> = Vec::new();

    for mode in &modes {
        println!("Testing mode: {}", mode.as_str());

        // Warmup run
        let _ = run_single_benchmark(&client, &warehouse, &namespace, &table, &filter, *mode).await;

        // Measured runs
        let mut mode_results: Vec<BenchmarkResult> = Vec::new();
        for i in 1..=3 {
            let result: BenchmarkResult =
                run_single_benchmark(&client, &warehouse, &namespace, &table, &filter, *mode)
                    .await?;
            println!(
                "  Run {}: {:?} ({} rows, {} bytes) [path: {}]",
                i, result.duration, result.rows_returned, result.bytes_transferred, result.scan_path
            );
            mode_results.push(result);
        }

        // Calculate average
        let avg_duration: Duration = Duration::from_nanos(
            mode_results
                .iter()
                .map(|r| r.duration.as_nanos())
                .sum::<u128>() as u64
                / 3,
        );
        let avg_rows: usize = mode_results.iter().map(|r| r.rows_returned).sum::<usize>() / 3;
        let avg_bytes: usize = mode_results
            .iter()
            .map(|r| r.bytes_transferred)
            .sum::<usize>()
            / 3;

        let scan_path: String = mode_results
            .first()
            .map(|r| r.scan_path.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let avg_result: BenchmarkResult = BenchmarkResult {
            mode: *mode,
            duration: avg_duration,
            rows_returned: avg_rows,
            bytes_transferred: avg_bytes,
            scan_path,
        };

        println!(
            "  Average: {:?} ({:.2} MB/s)\n",
            avg_duration,
            avg_result.throughput_mbps()
        );
        results.push(avg_result);
    }

    // Print summary
    println!("==============================================");
    println!("                   RESULTS");
    println!("==============================================\n");

    println!(
        "{:<20} {:>12} {:>12} {:>12}",
        "Mode", "Time", "Rows", "Throughput"
    );
    println!(
        "{:<20} {:>12} {:>12} {:>12}",
        "----", "----", "----", "----------"
    );

    for result in &results {
        println!(
            "{:<20} {:>12?} {:>12} {:>10.2} MB/s",
            result.mode.as_str(),
            result.duration,
            result.rows_returned,
            result.throughput_mbps()
        );
    }

    // Calculate speedup
    if results.len() >= 2 {
        let standard_time: f64 = results[0].duration.as_secs_f64();
        let pushdown_time: f64 = results[1].duration.as_secs_f64();

        if pushdown_time > 0.0 {
            let speedup: f64 = standard_time / pushdown_time;
            println!();
            println!("Storage Pushdown Speedup: {:.2}x", speedup);

            if speedup > 1.0 {
                println!(
                    "  Storage Pushdown is {:.1}% faster than Standard",
                    (speedup - 1.0) * 100.0
                );
            } else if speedup < 1.0 {
                println!(
                    "  Standard is {:.1}% faster than Storage Pushdown",
                    (1.0 / speedup - 1.0) * 100.0
                );
            } else {
                println!("  Both modes perform similarly");
            }
        }
    }

    println!();
    Ok(())
}

async fn run_single_benchmark(
    client: &TablesClient,
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
    filter: &minio::s3tables::filter::Filter,
    mode: PushdownMode,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let start: Instant = Instant::now();

    let use_pushdown: bool = matches!(mode, PushdownMode::StoragePushdown);

    let response: minio::s3tables::response::ExecuteTableScanResponse = client
        .execute_table_scan(warehouse.clone(), namespace.clone(), table.clone())
        .filter(filter.to_json())
        .simd_mode(SimdMode::Avx512)
        .output_format(OutputFormat::JsonLines)
        .storage_pushdown(use_pushdown)
        .build()
        .send()
        .await?;

    let duration: Duration = start.elapsed();
    let bytes_transferred: usize = response.body_size();
    let rows_returned: usize = response.row_count()?;
    let scan_path: String = response.scan_path().unwrap_or("unknown").to_string();


    Ok(BenchmarkResult {
        mode,
        duration,
        rows_returned,
        bytes_transferred,
        scan_path,
    })
}

async fn check_table_exists(
    client: &TablesClient,
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
) -> bool {
    client
        .table_exists(warehouse.clone(), namespace.clone(), table.clone())
        .build()
        .send()
        .await
        .map(|resp| resp.exists())
        .unwrap_or(false)
}

async fn setup_test_data(
    client: &TablesClient,
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up benchmark test data...");

    // Create warehouse (ignore error if exists)
    let _ = client
        .create_warehouse(warehouse.clone())
        .build()
        .send()
        .await;
    println!("  Created warehouse: {}", warehouse.as_str());

    // Create namespace (ignore error if exists)
    let _ = client
        .create_namespace(warehouse.clone(), namespace.clone())
        .build()
        .send()
        .await;
    println!("  Created namespace: {:?}", namespace.as_slice());

    // Define schema with string fields for ILIKE testing
    let schema: Schema = Schema {
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 2,
                name: "name".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Name field for ILIKE testing".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 3,
                name: "description".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Description field for ILIKE testing".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 4,
                name: "category".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Category field".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    };

    // Create table
    let table_resp: minio::s3tables::response::CreateTableResponse = client
        .create_table(warehouse.clone(), namespace.clone(), table.clone(), schema)
        .build()
        .send()
        .await?;

    let metadata_location: String = table_resp
        .table_result()?
        .metadata_location
        .unwrap_or_default();
    println!(
        "  Created table: {} ({})",
        table.as_str(),
        metadata_location
    );

    println!();
    println!("NOTE: To run meaningful benchmarks, please populate the table with data");
    println!("      containing string values for ILIKE pattern matching.");
    println!();

    Ok(())
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
