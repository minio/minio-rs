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

//! Stress test: S3 Tables throughput saturation analysis.
//!
//! This test gradually increases concurrent client count to find the saturation
//! point where performance begins to degrade. Outputs detailed metrics to CSV
//! for visualization and analysis.
//!
//! # Critical Questions Answered
//!
//! 1. At what concurrent client count does latency exceed 500ms?
//! 2. Does performance degrade linearly or exponentially with load?
//! 3. What is the peak sustainable throughput?
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
//! - `NUM_VIEWS_PER_NS`: Views created per namespace (default: 2)
//!
//! # Operation Mix (Read-Heavy, Supported Operations Only)
//!
//! | Operation | Percentage | Description |
//! |-----------|------------|-------------|
//! | load_table | 35% | Load table metadata |
//! | list_tables | 25% | List tables in namespace |
//! | list_namespaces | 15% | List namespaces in warehouse |
//! | get_warehouse | 10% | Get warehouse details |
//! | load_view | 10% | Load view metadata |
//! | list_views | 5% | List views in namespace |
//!
//! # Output
//!
//! Creates `tables_throughput_saturation.csv` with columns:
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
//! - MinIO AIStor server at http://localhost:9000
//! - Admin credentials: minioadmin/minioadmin

use minio::s3::types::Region;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{Namespace, TableName, ViewName, ViewSql, WarehouseName};
use minio::s3tables::{TablesApi, TablesClient};
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
const NUM_NAMESPACES: usize = 3;
const NUM_TABLES_PER_NS: usize = 5;
const NUM_VIEWS_PER_NS: usize = 2;

/// Single operation measurement for latency tracking.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct OperationMetric {
    timestamp: Instant,
    duration_ms: u64,
    success: bool,
    operation: String,
}

/// Thread-safe metrics collector for aggregating operation statistics.
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

    fn record(&self, start: Instant, success: bool, operation: &str) {
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
            operation: operation.to_string(),
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
        let latency_p99_ms = latencies[latencies.len().saturating_sub(1) * 99 / 100];

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
}

/// Aggregated statistics computed from collected metrics for a measurement window.
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

/// Information about a table for random selection during load testing.
#[derive(Clone)]
struct TableInfo {
    warehouse: WarehouseName,
    namespace: Namespace,
    table_name: TableName,
}

/// Information about a view for random selection during load testing.
#[derive(Clone)]
struct ViewInfo {
    warehouse: WarehouseName,
    namespace: Namespace,
    view_name: ViewName,
}

/// Worker task that executes random read operations with configurable mix.
/// Each iteration selects an operation, measures latency, and records metrics.
async fn client_task(
    tables: TablesClient,
    table_info: Vec<TableInfo>,
    view_info: Vec<ViewInfo>,
    warehouse: WarehouseName,
    namespaces: Vec<Namespace>,
    collector: Arc<MetricsCollector>,
    stop_signal: Arc<AtomicBool>,
) {
    while !stop_signal.load(Ordering::Relaxed) {
        let (operation, table_idx, view_idx, ns_idx, sleep_ms) = {
            let mut rng = rand::rng();
            let op = rng.random_range(0..20);
            let t_idx = rng.random_range(0..table_info.len());
            let v_idx = rng.random_range(0..view_info.len().max(1));
            let n_idx = rng.random_range(0..namespaces.len());
            let sleep = rng.random_range(10..30);
            (op, t_idx, v_idx, n_idx, sleep)
        };

        let tbl_info = &table_info[table_idx];
        let ns = &namespaces[ns_idx];

        let start = Instant::now();

        // Operation mix (all supported operations only):
        // 35% load_table, 25% list_tables, 15% list_namespaces,
        // 10% get_warehouse, 10% load_view, 5% list_views
        let (success, op_name) = if operation < 7 {
            // 35% load_table (0-6)
            let result = tables
                .load_table(
                    tbl_info.warehouse.clone(),
                    tbl_info.namespace.clone(),
                    tbl_info.table_name.clone(),
                )
                .build()
                .send()
                .await;
            (result.is_ok(), "load_table")
        } else if operation < 12 {
            // 25% list_tables (7-11)
            let result = tables
                .list_tables(warehouse.clone(), ns.clone())
                .build()
                .send()
                .await;
            (result.is_ok(), "list_tables")
        } else if operation < 15 {
            // 15% list_namespaces (12-14)
            let result = tables
                .list_namespaces(warehouse.clone())
                .build()
                .send()
                .await;
            (result.is_ok(), "list_namespaces")
        } else if operation < 17 {
            // 10% get_warehouse (15-16)
            let result = tables.get_warehouse(warehouse.clone()).build().send().await;
            (result.is_ok(), "get_warehouse")
        } else if operation < 19 {
            // 10% load_view (17-18)
            if !view_info.is_empty() {
                let v_info = &view_info[view_idx % view_info.len()];
                let result = tables
                    .load_view(
                        v_info.warehouse.clone(),
                        v_info.namespace.clone(),
                        v_info.view_name.clone(),
                    )
                    .build()
                    .send()
                    .await;
                (result.is_ok(), "load_view")
            } else {
                (true, "load_view_skip")
            }
        } else {
            // 5% list_views (19)
            let result = tables
                .list_views(warehouse.clone(), ns.clone())
                .build()
                .send()
                .await;
            (result.is_ok(), "list_views")
        };

        collector.record(start, success, op_name);

        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

/// Creates an Iceberg schema with id, timestamp, and data fields for testing.
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
    println!("=== S3 Tables Stress Test: Throughput Saturation Analysis ===\n");
    println!("Configuration:");
    println!("  Start clients:        {}", START_CLIENTS);
    println!("  Client increment:     {}", CLIENT_INCREMENT);
    println!("  Max clients:          {}", MAX_CLIENTS);
    println!(
        "  Measurement window:   {} seconds",
        MEASUREMENT_WINDOW_SECS
    );
    println!("  Operation mix (supported operations only):");
    println!("    35% load_table, 25% list_tables, 15% list_namespaces");
    println!("    10% get_warehouse, 10% load_view, 5% list_views\n");

    let tables = TablesClient::builder()
        .endpoint("http://localhost:9000")
        .credentials("minioadmin", "minioadmin")
        .region(Region::try_from("us-east-1").unwrap())
        .build()?;

    let warehouse = WarehouseName::try_from("saturation-test-wh")?;
    println!("Step 1: Creating test infrastructure...");

    // Create warehouse
    match tables
        .create_warehouse(warehouse.clone())
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Created warehouse: {}", warehouse.as_str()),
        Err(e) => {
            println!("  Note: Warehouse may already exist: {}", e);
        }
    }

    // Create namespaces, tables, and views
    let mut namespaces: Vec<Namespace> = Vec::new();
    let mut table_info: Vec<TableInfo> = Vec::new();
    let mut view_info: Vec<ViewInfo> = Vec::new();
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

        // Create views
        for view_idx in 0..NUM_VIEWS_PER_NS {
            let view_name_str = format!("view{}_{}", ns_idx, view_idx);
            let view_name = ViewName::try_from(view_name_str.as_str())?;
            let view_sql = ViewSql::new("SELECT * FROM dummy")?;
            match tables
                .create_view(
                    warehouse.clone(),
                    namespace.clone(),
                    view_name.clone(),
                    schema.clone(),
                    view_sql,
                )
                .build()
                .send()
                .await
            {
                Ok(_) => println!("    Created view: {}.{}", ns_name, view_name_str),
                Err(e) => println!("    Note: View may already exist: {}", e),
            }

            view_info.push(ViewInfo {
                warehouse: warehouse.clone(),
                namespace: namespace.clone(),
                view_name,
            });
        }
    }
    println!(
        "  Created {} namespaces with {} tables and {} views total\n",
        NUM_NAMESPACES,
        table_info.len(),
        view_info.len()
    );

    let csv_filename = "tables_throughput_saturation.csv";
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
            let tables_clone = TablesClient::builder()
                .endpoint("http://localhost:9000")
                .credentials("minioadmin", "minioadmin")
                .region(Region::try_from("us-east-1").unwrap())
                .build()?;
            let table_info_clone = table_info.clone();
            let view_info_clone = view_info.clone();
            let namespaces_clone = namespaces.clone();
            let warehouse_clone = warehouse.clone();
            let collector_clone = Arc::clone(&collector);
            let stop_signal_clone = Arc::clone(&stop_signal);

            tasks.spawn(async move {
                client_task(
                    tables_clone,
                    table_info_clone,
                    view_info_clone,
                    warehouse_clone,
                    namespaces_clone,
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
    }

    println!("\n=== Test Complete ===");
    println!("Results written to: {}", csv_filename);
    println!("\nSummary:");

    let max_throughput = results
        .iter()
        .map(|(_, s)| s.throughput)
        .fold(0.0, f64::max);
    let max_clients = results.last().map(|(c, _)| *c).unwrap_or(0);
    let max_p99 = results
        .iter()
        .map(|(_, s)| s.latency_p99_ms)
        .max()
        .unwrap_or(0);

    println!("  - Peak throughput: {:.2} ops/sec", max_throughput);
    println!("  - Max clients tested: {}", max_clients);
    println!("  - Max P99 latency: {} ms", max_p99);

    let throughput_growth = results
        .iter()
        .map(|(c, s)| s.throughput / (*c as f64))
        .collect::<Vec<_>>();

    if !throughput_growth.is_empty() && throughput_growth[0] > 0.0 {
        let mean_tpc = throughput_growth.iter().sum::<f64>() / throughput_growth.len() as f64;
        let variance = throughput_growth
            .iter()
            .map(|x| (x - mean_tpc).powi(2))
            .sum::<f64>()
            / throughput_growth.len() as f64;
        let std_dev = variance.sqrt();
        let cv = (std_dev / mean_tpc) * 100.0;

        println!(
            "  - Throughput per client: {:.2} ops/sec/client (CV: {:.1}%)",
            mean_tpc, cv
        );
    }

    println!("\nRun visualization script:");
    println!("  python examples/s3tables/plot_tables_saturation.py");

    Ok(())
}
