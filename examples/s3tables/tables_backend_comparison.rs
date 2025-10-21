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

//! Backend Comparison Test for Iceberg REST Catalog Implementations
//!
//! This example runs identical operations against multiple Iceberg REST Catalog
//! backends (MinIO, Polaris, etc.) and compares their responses for conformance testing.
//!
//! # Prerequisites
//!
//! 1. MinIO AIStor running on localhost:9000
//! 2. Apache Polaris running on localhost:8181 (via Docker or standalone)
//!
//! # Usage
//!
//! ```bash
//! # Start MinIO
//! MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio server /data
//!
//! # Start Polaris (Docker)
//! docker run -p 8181:8181 apache/polaris:latest
//!
//! # Run comparison test
//! cargo run --example tables_backend_comparison
//! ```
//!
//! # How It Works
//!
//! 1. Creates TablesClient instances for each backend
//! 2. Runs identical random operations on all backends
//! 3. Compares success/failure status and response data
//! 4. Generates a compatibility report

use minio::s3tables::auth::{NoAuth, SigV4Auth};
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio::s3tables::{HasProperties, HasTableResult, TablesApi, TablesClient};
use rand::Rng;
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

/// Configuration for a backend
#[derive(Clone)]
struct BackendConfig {
    name: String,
    endpoint: String,
    base_path: String,
    warehouse: String,
    use_sigv4: bool,
    access_key: Option<String>,
    secret_key: Option<String>,
    supports_warehouse_api: bool,
}

/// Result of a single operation on one backend
#[derive(Debug)]
struct OperationResult {
    backend: String,
    success: bool,
    error_message: Option<String>,
    latency_ms: u64,
    response_summary: String,
}

/// Comparison of results across backends
struct ComparisonReport {
    operation: String,
    results: Vec<OperationResult>,
    all_agree: bool,
    differences: Vec<String>,
}

impl fmt::Display for ComparisonReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Operation: {}", self.operation)?;
        writeln!(f, "  Results:")?;
        for result in &self.results {
            let status = if result.success { "OK" } else { "FAIL" };
            writeln!(
                f,
                "    [{:12}] {} ({} ms) - {}",
                result.backend, status, result.latency_ms, result.response_summary
            )?;
            if let Some(err) = &result.error_message {
                writeln!(f, "               Error: {}", err)?;
            }
        }
        if self.all_agree {
            writeln!(f, "  Status: MATCH")?;
        } else {
            writeln!(f, "  Status: DIVERGENCE")?;
            for diff in &self.differences {
                writeln!(f, "    - {}", diff)?;
            }
        }
        Ok(())
    }
}

/// Operations that can be tested
#[derive(Debug, Clone)]
enum Operation {
    ListNamespaces,
    CreateNamespace(String),
    GetNamespace(String),
    DeleteNamespace(String),
    ListTables(String),
    CreateTable(String, String),
    LoadTable(String, String),
    DeleteTable(String, String),
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::ListNamespaces => write!(f, "list_namespaces"),
            Operation::CreateNamespace(ns) => write!(f, "create_namespace({})", ns),
            Operation::GetNamespace(ns) => write!(f, "get_namespace({})", ns),
            Operation::DeleteNamespace(ns) => write!(f, "delete_namespace({})", ns),
            Operation::ListTables(ns) => write!(f, "list_tables({})", ns),
            Operation::CreateTable(ns, t) => write!(f, "create_table({}, {})", ns, t),
            Operation::LoadTable(ns, t) => write!(f, "load_table({}, {})", ns, t),
            Operation::DeleteTable(ns, t) => write!(f, "delete_table({}, {})", ns, t),
        }
    }
}

/// Create a test schema
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
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Data field".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    }
}

/// Multi-backend comparator
struct BackendComparator {
    backends: Vec<(BackendConfig, TablesClient)>,
}

impl BackendComparator {
    async fn new(configs: Vec<BackendConfig>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut backends = Vec::new();

        for config in configs {
            let client = if config.use_sigv4 {
                let auth = SigV4Auth::new(
                    config.access_key.as_deref().unwrap_or("minioadmin"),
                    config.secret_key.as_deref().unwrap_or("minioadmin"),
                );
                TablesClient::builder()
                    .endpoint(&config.endpoint)
                    .base_path(&config.base_path)
                    .region("us-east-1")
                    .auth(auth)
                    .build()?
            } else {
                TablesClient::builder()
                    .endpoint(&config.endpoint)
                    .base_path(&config.base_path)
                    .region("us-east-1")
                    .auth(NoAuth::new())
                    .build()?
            };
            backends.push((config, client));
        }

        Ok(Self { backends })
    }

    /// Setup warehouses on all backends that support it
    async fn setup_warehouses(&self) {
        println!("Setting up warehouses...");
        for (config, client) in &self.backends {
            if config.supports_warehouse_api {
                print!(
                    "  Creating warehouse '{}' on {}... ",
                    config.warehouse, config.name
                );
                let wh = WarehouseName::try_from(config.warehouse.as_str()).ok();
                if let Some(warehouse) = wh {
                    match client.create_warehouse(warehouse).build().send().await {
                        Ok(_) => println!("OK"),
                        Err(e) => {
                            let err_str = e.to_string();
                            if err_str.contains("already exists") {
                                println!("already exists (OK)");
                            } else {
                                println!("WARN: {}", err_str);
                            }
                        }
                    }
                } else {
                    println!("WARN: Invalid warehouse name");
                }
            } else {
                println!(
                    "  Skipping warehouse creation on {} (pre-configured)",
                    config.name
                );
            }
        }
        println!();
    }

    /// Cleanup warehouses on all backends that support it
    async fn cleanup_warehouses(&self) {
        println!("\nCleaning up warehouses...");
        for (config, client) in &self.backends {
            if config.supports_warehouse_api {
                print!(
                    "  Deleting warehouse '{}' on {}... ",
                    config.warehouse, config.name
                );
                let wh = WarehouseName::try_from(config.warehouse.as_str()).ok();
                if let Some(warehouse) = wh {
                    match client.delete_warehouse(warehouse).build().send().await {
                        Ok(_) => println!("OK"),
                        Err(e) => println!("WARN: {}", e),
                    }
                } else {
                    println!("WARN: Invalid warehouse name");
                }
            }
        }
    }

    /// Run an operation on all backends and compare results
    async fn compare(&self, operation: &Operation) -> ComparisonReport {
        let mut results = Vec::new();

        for (config, client) in &self.backends {
            let start = Instant::now();
            let result = self.execute_operation(client, config, operation).await;
            let latency = start.elapsed().as_millis() as u64;

            let (success, error_message, response_summary) = match result {
                Ok(summary) => (true, None, summary),
                Err(e) => (false, Some(e.to_string()), "error".to_string()),
            };

            results.push(OperationResult {
                backend: config.name.clone(),
                success,
                error_message,
                latency_ms: latency,
                response_summary,
            });
        }

        // Compare results
        let all_success = results.iter().all(|r| r.success);
        let all_fail = results.iter().all(|r| !r.success);
        let all_agree = all_success || all_fail;

        let differences = if !all_agree {
            results
                .iter()
                .filter(|r| r.success != results[0].success)
                .map(|r| {
                    format!(
                        "{} returned {} while {} returned {}",
                        r.backend,
                        if r.success { "success" } else { "error" },
                        results[0].backend,
                        if results[0].success {
                            "success"
                        } else {
                            "error"
                        }
                    )
                })
                .collect()
        } else {
            Vec::new()
        };

        ComparisonReport {
            operation: operation.to_string(),
            results,
            all_agree,
            differences,
        }
    }

    async fn execute_operation(
        &self,
        client: &TablesClient,
        config: &BackendConfig,
        operation: &Operation,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let warehouse = &config.warehouse;

        match operation {
            Operation::ListNamespaces => {
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let result = client.list_namespaces(wh).build().send().await?;
                let namespaces = result.namespaces()?;
                Ok(format!("{} namespaces", namespaces.len()))
            }
            Operation::CreateNamespace(ns) => {
                let mut properties = HashMap::new();
                properties.insert("description".to_string(), "test namespace".to_string());
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                let _resp = client
                    .create_namespace(wh, namespace)
                    .properties(properties)
                    .build()
                    .send()
                    .await?;
                Ok("created".to_string())
            }
            Operation::GetNamespace(ns) => {
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                let result = client.get_namespace(wh, namespace).build().send().await?;
                let props = result.properties()?;
                Ok(format!("props: {}", props.len()))
            }
            Operation::DeleteNamespace(ns) => {
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                client
                    .delete_namespace(wh, namespace)
                    .build()
                    .send()
                    .await?;
                Ok("deleted".to_string())
            }
            Operation::ListTables(ns) => {
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                let result = client.list_tables(wh, namespace).build().send().await?;
                let identifiers = result.identifiers()?;
                Ok(format!("{} tables", identifiers.len()))
            }
            Operation::CreateTable(ns, table) => {
                let schema = create_test_schema();
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                let table_name = TableName::try_from(table.as_str())?;
                let _result = client
                    .create_table(wh, namespace, table_name, schema)
                    .build()
                    .send()
                    .await?;
                Ok("created".to_string())
            }
            Operation::LoadTable(ns, table) => {
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                let table_name = TableName::try_from(table.as_str())?;
                let result = client
                    .load_table(wh, namespace, table_name)
                    .build()
                    .send()
                    .await?;
                let table_result = result.table_result()?;
                Ok(format!(
                    "location: {}",
                    table_result.metadata_location.as_deref().unwrap_or("N/A")
                ))
            }
            Operation::DeleteTable(ns, table) => {
                let wh = WarehouseName::try_from(warehouse.as_str())?;
                let namespace = Namespace::try_from(vec![ns.clone()])?;
                let table_name = TableName::try_from(table.as_str())?;
                client
                    .delete_table(wh, namespace, table_name)
                    .build()
                    .send()
                    .await?;
                Ok("deleted".to_string())
            }
        }
    }

    /// Run a sequence of operations and report results
    async fn run_test_sequence(&self) -> Vec<ComparisonReport> {
        let mut reports = Vec::new();
        let test_ns = format!("comparison_test_{}", rand::rng().random::<u32>() % 100000);
        let test_table = "test_table";

        println!("=== Backend Comparison Test ===\n");
        println!("Backends under test:");
        for (config, _) in &self.backends {
            println!(
                "  - {} ({}{}) warehouse={}",
                config.name, config.endpoint, config.base_path, config.warehouse
            );
        }
        println!();

        // Test sequence: create -> use -> cleanup
        let operations = vec![
            Operation::ListNamespaces,
            Operation::CreateNamespace(test_ns.clone()),
            Operation::GetNamespace(test_ns.clone()),
            Operation::ListTables(test_ns.clone()),
            Operation::CreateTable(test_ns.clone(), test_table.to_string()),
            Operation::LoadTable(test_ns.clone(), test_table.to_string()),
            Operation::ListTables(test_ns.clone()),
            Operation::DeleteTable(test_ns.clone(), test_table.to_string()),
            Operation::DeleteNamespace(test_ns.clone()),
        ];

        for op in &operations {
            println!("Running: {}", op);
            let report = self.compare(op).await;
            println!("{}", report);
            reports.push(report);

            // Small delay between operations
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        reports
    }

    /// Run random operations for stress comparison
    async fn run_random_comparison(&self, iterations: usize) -> Vec<ComparisonReport> {
        let mut reports = Vec::new();
        let mut rng = rand::rng();
        let test_ns = format!("random_test_{}", rng.random::<u32>() % 100000);

        println!(
            "=== Random Operation Comparison ({} iterations) ===\n",
            iterations
        );

        // Setup: create namespace
        let setup_op = Operation::CreateNamespace(test_ns.clone());
        println!("Setup: {}", setup_op);
        let _ = self.compare(&setup_op).await;

        for i in 0..iterations {
            let op = match rng.random_range(0..5) {
                0 => Operation::ListNamespaces,
                1 => Operation::GetNamespace(test_ns.clone()),
                2 => Operation::ListTables(test_ns.clone()),
                3 => {
                    let table = format!("table_{}", rng.random::<u16>());
                    Operation::CreateTable(test_ns.clone(), table)
                }
                _ => Operation::ListNamespaces,
            };

            println!("[{}/{}] {}", i + 1, iterations, op);
            let report = self.compare(&op).await;

            if !report.all_agree {
                println!("  DIVERGENCE DETECTED!");
                for diff in &report.differences {
                    println!("    {}", diff);
                }
            }

            reports.push(report);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Cleanup
        println!("\nCleanup...");
        let _ = self.compare(&Operation::DeleteNamespace(test_ns)).await;

        reports
    }
}

/// Generate summary statistics
fn generate_summary(reports: &[ComparisonReport]) {
    println!("\n=== Comparison Summary ===\n");

    let total = reports.len();
    let matches = reports.iter().filter(|r| r.all_agree).count();
    let divergences = total - matches;

    println!("Total operations: {}", total);
    println!(
        "Matching results: {} ({:.1}%)",
        matches,
        100.0 * matches as f64 / total as f64
    );
    println!(
        "Divergences: {} ({:.1}%)",
        divergences,
        100.0 * divergences as f64 / total as f64
    );

    if divergences > 0 {
        println!("\nDivergent operations:");
        for report in reports.iter().filter(|r| !r.all_agree) {
            println!("  - {}", report.operation);
        }
    }

    // Latency comparison
    println!("\nLatency Summary (ms):");
    let mut backend_latencies: HashMap<String, Vec<u64>> = HashMap::new();
    for report in reports {
        for result in &report.results {
            backend_latencies
                .entry(result.backend.clone())
                .or_default()
                .push(result.latency_ms);
        }
    }

    for (backend, latencies) in &backend_latencies {
        let avg = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
        let min = latencies.iter().min().unwrap_or(&0);
        let max = latencies.iter().max().unwrap_or(&0);
        println!(
            "  {}: avg={:.1}ms, min={}ms, max={}ms",
            backend, avg, min, max
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check which backends are available
    let minio_available = check_backend("http://localhost:9000").await;
    let polaris_available = check_backend("http://localhost:8181").await;

    println!("Backend availability:");
    println!(
        "  MinIO (localhost:9000): {}",
        if minio_available { "UP" } else { "DOWN" }
    );
    println!(
        "  Polaris (localhost:8181): {}",
        if polaris_available { "UP" } else { "DOWN" }
    );
    println!();

    if !minio_available && !polaris_available {
        println!("ERROR: No backends available for testing.");
        println!();
        println!("To run this test, start at least one backend:");
        println!();
        println!("  MinIO:");
        println!("    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \\");
        println!("    ./minio server /data --console-address :9001");
        println!();
        println!("  Polaris (Docker):");
        println!("    docker run -p 8181:8181 apache/polaris:latest");
        println!();
        return Ok(());
    }

    // Generate unique warehouse name for this test run
    let warehouse_name = format!("comparison-test-{}", rand::rng().random::<u32>() % 100000);

    // Configure available backends
    let mut configs = Vec::new();

    if minio_available {
        configs.push(BackendConfig {
            name: "MinIO".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            base_path: "/_iceberg/v1".to_string(),
            warehouse: warehouse_name.clone(),
            use_sigv4: true,
            access_key: std::env::var("ACCESS_KEY")
                .ok()
                .or(Some("minioadmin".to_string())),
            secret_key: std::env::var("SECRET_KEY")
                .ok()
                .or(Some("minioadmin".to_string())),
            supports_warehouse_api: true,
        });
    }

    if polaris_available {
        configs.push(BackendConfig {
            name: "Polaris".to_string(),
            endpoint: "http://localhost:8181".to_string(),
            base_path: "/api/catalog/v1".to_string(),
            warehouse: "polaris-catalog".to_string(), // Polaris requires pre-configured catalog
            use_sigv4: false,
            access_key: None,
            secret_key: None,
            supports_warehouse_api: false, // Polaris catalogs must be pre-configured
        });
    }

    if configs.len() < 2 {
        println!(
            "NOTE: Only {} backend available. Comparison requires 2+ backends.",
            configs.len()
        );
        println!("Running single-backend test for validation...\n");
    }

    let comparator = BackendComparator::new(configs).await?;

    // Setup warehouses
    comparator.setup_warehouses().await;

    // Run structured test sequence
    let reports = comparator.run_test_sequence().await;
    generate_summary(&reports);

    // Optionally run random comparison
    println!("\n--- Press Enter to run random comparison (20 iterations) or Ctrl+C to exit ---");
    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_ok() {
        let random_reports = comparator.run_random_comparison(20).await;
        generate_summary(&random_reports);
    }

    // Cleanup warehouses
    comparator.cleanup_warehouses().await;

    Ok(())
}

/// Check if a backend is reachable
async fn check_backend(endpoint: &str) -> bool {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    client.get(endpoint).send().await.is_ok()
}
