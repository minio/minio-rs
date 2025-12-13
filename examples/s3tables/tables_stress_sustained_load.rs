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

//! Stress test: S3 Tables sustained high load analysis.
//!
//! This test runs at fixed high concurrency for extended duration to measure
//! performance stability over time. Outputs detailed time-series metrics to CSV.
//!
//! # Critical Questions Answered
//!
//! 1. How long can the system sustain peak load before degrading?
//! 2. Does throughput remain stable over time?
//! 3. Does latency increase over extended operation?
//!
//! # Test Approach
//!
//! 1. Run at fixed concurrency (50 clients by default)
//! 2. Sample metrics at regular intervals (10 seconds)
//! 3. Record window throughput, cumulative throughput, latency trends
//! 4. Continue for test duration (30 minutes by default)
//! 5. Export time-series metrics to CSV for trend analysis
//!
//! # Configuration
//!
//! - `CONCURRENT_CLIENTS`: Fixed concurrent clients (default: 50)
//! - `TEST_DURATION_SECS`: Total test duration (default: 1800 = 30 minutes)
//! - `SAMPLE_INTERVAL_SECS`: Sampling interval (default: 10)
//! - `OPERATION_MIX`: load_table:list_tables:list_namespaces:get_warehouse (40:30:20:10)
//!
//! # Output
//!
//! Creates `tables_sustained_load.csv` with columns:
//! - elapsed_secs: Time since test start
//! - sample_window_ops: Operations in this sample window
//! - window_throughput: Throughput in this window (ops/sec)
//! - cumulative_ops: Total operations so far
//! - cumulative_throughput: Average throughput over entire test
//! - latency_mean_ms, latency_p50_ms, latency_p95_ms, latency_p99_ms
//! - error_rate: Error rate in this window
//! - cumulative_error_rate: Total error rate
//!
//! # Requirements
//!
//! - MinIO AIStor server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin

use minio::s3::types::Region;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio::s3tables::{TablesApi, TablesClient};
use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const CONCURRENT_CLIENTS: usize = 50;
const TEST_DURATION_SECS: u64 = 1800; // 30 minutes
const SAMPLE_INTERVAL_SECS: u64 = 10;
const NUM_NAMESPACES: usize = 3;
const NUM_TABLES_PER_NS: usize = 5;

#[derive(Debug, Clone)]
struct OperationMetric {
    duration_ms: u64,
}

struct WindowMetrics {
    operations: Mutex<Vec<OperationMetric>>,
    op_counter: AtomicU64,
    error_counter: AtomicU64,
    cumulative_ops: AtomicU64,
    cumulative_errors: AtomicU64,
}

impl WindowMetrics {
    fn new() -> Self {
        Self {
            operations: Mutex::new(Vec::new()),
            op_counter: AtomicU64::new(0),
            error_counter: AtomicU64::new(0),
            cumulative_ops: AtomicU64::new(0),
            cumulative_errors: AtomicU64::new(0),
        }
    }

    fn record(&self, duration_ms: u64, success: bool) {
        if success {
            self.op_counter.fetch_add(1, Ordering::Relaxed);
            self.cumulative_ops.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error_counter.fetch_add(1, Ordering::Relaxed);
            self.cumulative_errors.fetch_add(1, Ordering::Relaxed);
        }

        let mut ops = self.operations.lock().unwrap();
        ops.push(OperationMetric { duration_ms });
    }

    fn take_window_stats(&self) -> WindowStats {
        let mut ops = self.operations.lock().unwrap();
        let window_ops = ops.drain(..).collect::<Vec<_>>();
        drop(ops);

        let window_success = self.op_counter.swap(0, Ordering::Relaxed);
        let window_errors = self.error_counter.swap(0, Ordering::Relaxed);

        if window_ops.is_empty() {
            return WindowStats {
                window_ops: 0,
                latency_mean_ms: 0.0,
                latency_p50_ms: 0,
                latency_p95_ms: 0,
                latency_p99_ms: 0,
                error_rate: 0.0,
            };
        }

        let mut latencies: Vec<u64> = window_ops.iter().map(|m| m.duration_ms).collect();
        latencies.sort_unstable();

        let latency_mean_ms = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
        let latency_p50_ms = latencies[latencies.len() * 50 / 100];
        let latency_p95_ms = latencies[latencies.len() * 95 / 100];
        let latency_p99_ms = latencies[latencies.len().saturating_sub(1) * 99 / 100];

        let total = window_success + window_errors;
        let error_rate = if total > 0 {
            window_errors as f64 / total as f64
        } else {
            0.0
        };

        WindowStats {
            window_ops: total,
            latency_mean_ms,
            latency_p50_ms,
            latency_p95_ms,
            latency_p99_ms,
            error_rate,
        }
    }

    fn cumulative_stats(&self) -> (u64, u64) {
        (
            self.cumulative_ops.load(Ordering::Relaxed),
            self.cumulative_errors.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug)]
struct WindowStats {
    window_ops: u64,
    latency_mean_ms: f64,
    latency_p50_ms: u64,
    latency_p95_ms: u64,
    latency_p99_ms: u64,
    error_rate: f64,
}

#[derive(Clone)]
struct TableInfo {
    warehouse: WarehouseName,
    namespace: Namespace,
    table_name: TableName,
}

async fn client_task(
    tables: TablesClient,
    table_info: Vec<TableInfo>,
    warehouse: WarehouseName,
    namespaces: Vec<Namespace>,
    metrics: Arc<WindowMetrics>,
    stop_signal: Arc<AtomicBool>,
) {
    while !stop_signal.load(Ordering::Relaxed) {
        let (operation, table_idx, ns_idx, sleep_ms) = {
            let mut rng = rand::rng();
            let op = rng.random_range(0..10);
            let t_idx = rng.random_range(0..table_info.len());
            let n_idx = rng.random_range(0..namespaces.len());
            let sleep = rng.random_range(10..30);
            (op, t_idx, n_idx, sleep)
        };

        let info = &table_info[table_idx];
        let ns = &namespaces[ns_idx];

        let start = Instant::now();

        let success = if operation < 4 {
            // 40% load_table
            tables
                .load_table(
                    info.warehouse.clone(),
                    info.namespace.clone(),
                    info.table_name.clone(),
                )
                .build()
                .send()
                .await
                .is_ok()
        } else if operation < 7 {
            // 30% list_tables
            tables
                .list_tables(warehouse.clone(), ns.clone())
                .build()
                .send()
                .await
                .is_ok()
        } else if operation < 9 {
            // 20% list_namespaces
            tables
                .list_namespaces(warehouse.clone())
                .build()
                .send()
                .await
                .is_ok()
        } else {
            // 10% get_warehouse
            tables
                .get_warehouse(warehouse.clone())
                .build()
                .send()
                .await
                .is_ok()
        };

        let duration_ms = start.elapsed().as_millis() as u64;
        metrics.record(duration_ms, success);

        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

fn create_test_schema() -> Schema {
    Schema {
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
                name: "timestamp".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Timestamptz),
                doc: Some("Record timestamp".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 3,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Data content".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== S3 Tables Stress Test: Sustained High Load Analysis ===\n");
    println!("Configuration:");
    println!("  Concurrent clients:   {}", CONCURRENT_CLIENTS);
    println!(
        "  Test duration:        {} seconds ({} minutes)",
        TEST_DURATION_SECS,
        TEST_DURATION_SECS / 60
    );
    println!("  Sample interval:      {} seconds", SAMPLE_INTERVAL_SECS);
    println!(
        "  Operation mix:        40% load_table, 30% list_tables, 20% list_namespaces, 10% get_warehouse\n"
    );

    let tables = TablesClient::builder()
        .endpoint("http://localhost:9000")
        .credentials("minioadmin", "minioadmin")
        .region(Region::try_from("us-east-1").unwrap())
        .build()?;

    let warehouse = WarehouseName::try_from("sustained-test-wh")?;
    println!("Step 1: Creating test infrastructure...");

    // Create warehouse
    match tables
        .create_warehouse(warehouse.clone())
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Created warehouse: {}", warehouse.as_str()),
        Err(e) => println!("  Note: Warehouse may already exist: {}", e),
    }

    // Create namespaces and tables
    let mut namespaces: Vec<Namespace> = Vec::new();
    let mut table_info: Vec<TableInfo> = Vec::new();
    let schema = create_test_schema();

    for ns_idx in 0..NUM_NAMESPACES {
        let ns_name = format!("ns{}", ns_idx);
        let namespace = Namespace::try_from(vec![ns_name.clone()])?;
        namespaces.push(namespace.clone());

        match tables
            .create_namespace(warehouse.clone(), namespace.clone())
            .build()
            .send()
            .await
        {
            Ok(_) => println!("  Created namespace: {}", ns_name),
            Err(e) => println!("  Note: Namespace may already exist: {}", e),
        }

        for tbl_idx in 0..NUM_TABLES_PER_NS {
            let table_name_str = format!("table{}_{}", ns_idx, tbl_idx);
            let table_name = TableName::try_from(table_name_str.as_str())?;
            match tables
                .create_table(
                    warehouse.clone(),
                    namespace.clone(),
                    table_name.clone(),
                    schema.clone(),
                )
                .build()
                .send()
                .await
            {
                Ok(_) => println!("    Created table: {}.{}", ns_name, table_name_str),
                Err(e) => println!("    Note: Table may already exist: {}", e),
            }

            table_info.push(TableInfo {
                warehouse: warehouse.clone(),
                namespace: namespace.clone(),
                table_name,
            });
        }
    }
    println!(
        "  Created {} namespaces with {} tables total\n",
        NUM_NAMESPACES,
        table_info.len()
    );

    let csv_filename = "tables_sustained_load.csv";
    let mut csv_file = File::create(csv_filename)?;
    writeln!(
        csv_file,
        "elapsed_secs,sample_window_ops,window_throughput,cumulative_ops,cumulative_throughput,latency_mean_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,error_rate,cumulative_error_rate"
    )?;

    println!("Step 2: Starting sustained load test...\n");
    let test_start = Instant::now();

    let metrics = Arc::new(WindowMetrics::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    // Spawn client tasks
    for _ in 0..CONCURRENT_CLIENTS {
        let tables_clone = TablesClient::builder()
            .endpoint("http://localhost:9000")
            .credentials("minioadmin", "minioadmin")
            .region(Region::try_from("us-east-1").unwrap())
            .build()?;
        let table_info_clone = table_info.clone();
        let namespaces_clone = namespaces.clone();
        let warehouse_clone = warehouse.clone();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            client_task(
                tables_clone,
                table_info_clone,
                warehouse_clone,
                namespaces_clone,
                metrics_clone,
                stop_signal_clone,
            )
            .await;
        });
    }

    // Sample metrics at intervals
    let mut sample_count = 0;
    let total_samples = TEST_DURATION_SECS / SAMPLE_INTERVAL_SECS;

    while test_start.elapsed().as_secs() < TEST_DURATION_SECS {
        tokio::time::sleep(Duration::from_secs(SAMPLE_INTERVAL_SECS)).await;
        sample_count += 1;

        let elapsed_secs = test_start.elapsed().as_secs_f64();
        let window_stats = metrics.take_window_stats();
        let (cumulative_ops, cumulative_errors) = metrics.cumulative_stats();

        let window_throughput = window_stats.window_ops as f64 / SAMPLE_INTERVAL_SECS as f64;
        let cumulative_throughput = cumulative_ops as f64 / elapsed_secs;
        let cumulative_total = cumulative_ops + cumulative_errors;
        let cumulative_error_rate = if cumulative_total > 0 {
            cumulative_errors as f64 / cumulative_total as f64
        } else {
            0.0
        };

        println!(
            "[Sample {}/{}] Elapsed: {:.0}s",
            sample_count, total_samples, elapsed_secs
        );
        println!(
            "  Window:     {} ops, {:.2} ops/sec",
            window_stats.window_ops, window_throughput
        );
        println!(
            "  Cumulative: {} ops, {:.2} ops/sec",
            cumulative_ops, cumulative_throughput
        );
        println!(
            "  Latency:    P50={}ms, P95={}ms, P99={}ms",
            window_stats.latency_p50_ms, window_stats.latency_p95_ms, window_stats.latency_p99_ms
        );
        println!(
            "  Error rate: {:.2}% (cumulative: {:.2}%)\n",
            window_stats.error_rate * 100.0,
            cumulative_error_rate * 100.0
        );

        writeln!(
            csv_file,
            "{:.2},{},{:.2},{},{:.2},{:.2},{},{},{},{:.4},{:.4}",
            elapsed_secs,
            window_stats.window_ops,
            window_throughput,
            cumulative_ops,
            cumulative_throughput,
            window_stats.latency_mean_ms,
            window_stats.latency_p50_ms,
            window_stats.latency_p95_ms,
            window_stats.latency_p99_ms,
            window_stats.error_rate,
            cumulative_error_rate
        )?;
        csv_file.flush()?;

        // Early termination conditions
        if window_stats.error_rate > 0.2 {
            println!("Warning: Window error rate exceeded 20% - stopping test early");
            break;
        }
        if window_stats.latency_p99_ms > 5000 {
            println!("Warning: P99 latency exceeded 5000ms - stopping test early");
            break;
        }
    }

    // Stop all client tasks
    stop_signal.store(true, Ordering::Relaxed);
    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Client task error: {}", e);
        }
    }

    let (final_ops, final_errors) = metrics.cumulative_stats();
    let final_elapsed = test_start.elapsed().as_secs_f64();
    let final_throughput = final_ops as f64 / final_elapsed;
    let final_total = final_ops + final_errors;
    let final_error_rate = if final_total > 0 {
        final_errors as f64 / final_total as f64
    } else {
        0.0
    };

    println!("\n=== Test Complete ===");
    println!("Results written to: {}", csv_filename);
    println!("\nFinal Summary:");
    println!("  Total duration:      {:.2} seconds", final_elapsed);
    println!("  Total operations:    {}", final_ops);
    println!("  Average throughput:  {:.2} ops/sec", final_throughput);
    println!("  Total errors:        {}", final_errors);
    println!("  Overall error rate:  {:.2}%", final_error_rate * 100.0);

    println!("\nRun visualization script:");
    println!("  python examples/s3tables/plot_tables_sustained.py");

    Ok(())
}
