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

//! Stress test: Ultra-aggressive state transition chaos test for S3 Tables.
//!
//! This test pushes the S3 Tables (Iceberg REST Catalog) API to its limits by
//! creating, deleting, and modifying warehouses, namespaces, tables, and views
//! in a chaotic manner. The goal is to find race conditions, deadlocks, and
//! breaking points that only appear under extreme concurrent load.
//!
//! # Test Scenario
//!
//! 1. Create initial warehouses, namespaces, and tables
//! 2. Spawn multiple threads that aggressively perform 33 different operations:
//!
//!    **Warehouse Operations (4)**
//!    - CreateWarehouse, DeleteWarehouse, ListWarehouses, GetWarehouse
//!
//!    **Namespace Operations (5)**
//!    - CreateNamespace, DeleteNamespace, ListNamespaces, GetNamespace, NamespaceExists
//!
//!    **Table Operations (8)**
//!    - CreateTable, DeleteTable, RenameTable, LoadTable, ListTables
//!    - TableExists, CommitTable, RegisterTable
//!
//!    **Multi-Table Operations (1)**
//!    - CommitMultiTableTransaction
//!
//!    **Config & Metrics (2)**
//!    - GetConfig, TableMetrics
//!
//!    **Namespace Properties (1)**
//!    - UpdateNamespaceProperties
//!
//!    **Credentials (1)**
//!    - LoadTableCredentials
//!
//!    **View Operations (7)**
//!    - CreateView, ListViews, LoadView, ReplaceView, DropView, ViewExists, RenameView
//!
//!    **Scan Planning Operations (4) - Iceberg V3 APIs**
//!    - PlanTableScan, FetchPlanningResult, FetchScanTasks, CancelPlanning
//!
//! 3. Run for sustained duration of chaos (default: 60 seconds)
//! 4. Monitor for:
//!    - Deadlocks (operations taking > 5 seconds)
//!    - Race conditions (inconsistent state)
//!    - Server crashes
//!    - API errors or panics
//!
//! # Expected Behavior
//!
//! - System should remain stable under chaos
//! - All operations should eventually complete or fail gracefully
//! - No deadlocks or stuck states
//! - No server crashes
//! - Errors are acceptable but system should recover
//! - High error rates are normal due to concurrent creates/deletes
//!
//! # Environment Variables
//!
//! | Variable | Default | Description |
//! |----------|---------|-------------|
//! | `TABLES_ENDPOINT` | `http://localhost:9000` | Server endpoint |
//! | `ACCESS_KEY` | `minioadmin` | Access key for SigV4 auth |
//! | `SECRET_KEY` | `minioadmin` | Secret key for SigV4 auth |
//! | `TABLES_REGION` | `us-east-1` | AWS region for signing |
//! | `TABLES_BASE_PATH` | `/_iceberg/v1` | API base path |
//! | `CLEANUP_AFTER_TEST` | unset | If set, cleanup warehouses after test |
//!
//! # Example Usage
//!
//! ```bash
//! # Run with defaults (local MinIO)
//! cargo run --example tables_stress_state_chaos
//!
//! # Run against different endpoint with cleanup
//! TABLES_ENDPOINT=http://minio.local:9000 CLEANUP_AFTER_TEST=1 cargo run --example tables_stress_state_chaos
//! ```

use minio::s3::types::Region;
use minio::s3tables::builders::{TableChange, TableIdentifier};
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{
    MetadataLocation, Namespace, PlanId as PlanIdType, TableName, ViewName, ViewSql, WarehouseName,
};
use minio::s3tables::{DEFAULT_BASE_PATH, HasTableResult, TablesApi, TablesClient, base_paths};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// Configuration from environment variables
struct Config {
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: Region,
    base_path: String,
}

impl Config {
    fn from_env() -> Self {
        let region_str = env::var("TABLES_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        Self {
            endpoint: env::var("TABLES_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            access_key: env::var("ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
            secret_key: env::var("SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string()),
            region: Region::try_from(region_str.as_str()).expect("invalid region"),
            base_path: env::var("TABLES_BASE_PATH")
                .unwrap_or_else(|_| DEFAULT_BASE_PATH.to_string()),
        }
    }

    fn create_client(&self) -> Result<TablesClient, Box<dyn std::error::Error + Send + Sync>> {
        Ok(TablesClient::builder()
            .endpoint(&self.endpoint)
            .credentials(&self.access_key, &self.secret_key)
            .region(self.region.clone())
            .base_path(&self.base_path)
            .build()?)
    }

    fn print(&self) {
        println!("  Endpoint:               {}", self.endpoint);
        println!("  Region:                 {}", self.region);
        println!("  Base path:              {}", self.base_path);
        println!("  Auth type:              SigV4Auth");
        println!();
        println!("  Known base paths:");
        println!("    MinIO/AWS:  {}", base_paths::MINIO_AWS);
    }
}

/// Type alias for namespace identifier (warehouse, namespace_path)
type NamespaceId = (String, Vec<String>);
/// Type alias for table identifier (warehouse, namespace_path, table_name)
type TableId = (String, Vec<String>, String);
/// Type alias for view identifier (warehouse, namespace_path, view_name)
type ViewId = (String, Vec<String>, String);
/// Type alias for plan identifier (warehouse, namespace_path, table_name, plan_id)
type PlanId = (String, Vec<String>, String, String);

const NUM_INITIAL_WAREHOUSES: usize = 5;
const NUM_INITIAL_NAMESPACES_PER_WH: usize = 3;
const NUM_INITIAL_TABLES_PER_NS: usize = 3;
const NUM_CHAOS_THREADS: usize = 15;
const TEST_DURATION_SECS: u64 = 60; // 1 minute for quick test
const MIN_SLEEP_MS: u64 = 10;
const MAX_SLEEP_MS: u64 = 50;

/// All possible chaos operations that can be randomly selected during the test.
/// Each variant maps to one Iceberg REST Catalog API operation.
#[derive(Debug, Clone, Copy)]
enum ChaosOperation {
    // Warehouse operations
    CreateWarehouse,
    DeleteWarehouse,
    ListWarehouses,
    GetWarehouse,
    // Namespace operations
    CreateNamespace,
    DeleteNamespace,
    ListNamespaces,
    GetNamespace,
    NamespaceExists,
    // Table operations
    CreateTable,
    DeleteTable,
    RenameTable,
    LoadTable,
    ListTables,
    TableExists,
    CommitTable,
    RegisterTable,
    // Multi-table operations
    CommitMultiTableTransaction,
    // Config & Metrics
    GetConfig,
    TableMetrics,
    // Namespace property operations
    UpdateNamespaceProperties,
    // Credentials operations
    LoadTableCredentials,
    // View operations
    CreateView,
    ListViews,
    LoadView,
    ReplaceView,
    DropView,
    ViewExists,
    RenameView,
    // Scan planning operations (V3)
    PlanTableScan,
    FetchPlanningResult,
    FetchScanTasks,
    CancelPlanning,
}

impl ChaosOperation {
    fn random() -> Self {
        let mut rng = rand::rng();
        match rng.random_range(0..33) {
            // Warehouse operations (4)
            0 => ChaosOperation::CreateWarehouse,
            1 => ChaosOperation::DeleteWarehouse,
            2 => ChaosOperation::ListWarehouses,
            3 => ChaosOperation::GetWarehouse,
            // Namespace operations (5)
            4 => ChaosOperation::CreateNamespace,
            5 => ChaosOperation::DeleteNamespace,
            6 => ChaosOperation::ListNamespaces,
            7 => ChaosOperation::GetNamespace,
            8 => ChaosOperation::NamespaceExists,
            // Table operations (8)
            9 => ChaosOperation::CreateTable,
            10 => ChaosOperation::DeleteTable,
            11 => ChaosOperation::RenameTable,
            12 => ChaosOperation::LoadTable,
            13 => ChaosOperation::ListTables,
            14 => ChaosOperation::TableExists,
            15 => ChaosOperation::CommitTable,
            16 => ChaosOperation::RegisterTable,
            // Multi-table operations (1)
            17 => ChaosOperation::CommitMultiTableTransaction,
            // Config & Metrics (2)
            18 => ChaosOperation::GetConfig,
            19 => ChaosOperation::TableMetrics,
            // Namespace property operations (1)
            20 => ChaosOperation::UpdateNamespaceProperties,
            // Credentials operations (1)
            21 => ChaosOperation::LoadTableCredentials,
            // View operations (7)
            22 => ChaosOperation::CreateView,
            23 => ChaosOperation::ListViews,
            24 => ChaosOperation::LoadView,
            25 => ChaosOperation::ReplaceView,
            26 => ChaosOperation::DropView,
            27 => ChaosOperation::ViewExists,
            28 => ChaosOperation::RenameView,
            // Scan planning operations (4) - V3 APIs
            29 => ChaosOperation::PlanTableScan,
            30 => ChaosOperation::FetchPlanningResult,
            31 => ChaosOperation::FetchScanTasks,
            _ => ChaosOperation::CancelPlanning,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ChaosOperation::CreateWarehouse => "CreateWarehouse",
            ChaosOperation::DeleteWarehouse => "DeleteWarehouse",
            ChaosOperation::ListWarehouses => "ListWarehouses",
            ChaosOperation::GetWarehouse => "GetWarehouse",
            ChaosOperation::CreateNamespace => "CreateNamespace",
            ChaosOperation::DeleteNamespace => "DeleteNamespace",
            ChaosOperation::ListNamespaces => "ListNamespaces",
            ChaosOperation::GetNamespace => "GetNamespace",
            ChaosOperation::NamespaceExists => "NamespaceExists",
            ChaosOperation::CreateTable => "CreateTable",
            ChaosOperation::DeleteTable => "DeleteTable",
            ChaosOperation::RenameTable => "RenameTable",
            ChaosOperation::LoadTable => "LoadTable",
            ChaosOperation::ListTables => "ListTables",
            ChaosOperation::TableExists => "TableExists",
            ChaosOperation::CommitTable => "CommitTable",
            ChaosOperation::RegisterTable => "RegisterTable",
            ChaosOperation::CommitMultiTableTransaction => "CommitMultiTx",
            ChaosOperation::GetConfig => "GetConfig",
            ChaosOperation::TableMetrics => "TableMetrics",
            ChaosOperation::UpdateNamespaceProperties => "UpdateNsProps",
            ChaosOperation::LoadTableCredentials => "LoadTableCreds",
            ChaosOperation::CreateView => "CreateView",
            ChaosOperation::ListViews => "ListViews",
            ChaosOperation::LoadView => "LoadView",
            ChaosOperation::ReplaceView => "ReplaceView",
            ChaosOperation::DropView => "DropView",
            ChaosOperation::ViewExists => "ViewExists",
            ChaosOperation::RenameView => "RenameView",
            ChaosOperation::PlanTableScan => "PlanTableScan",
            ChaosOperation::FetchPlanningResult => "FetchPlanResult",
            ChaosOperation::FetchScanTasks => "FetchScanTasks",
            ChaosOperation::CancelPlanning => "CancelPlanning",
        }
    }
}

/// Maximum number of recent errors to retain for reporting.
const MAX_RECENT_ERRORS: usize = 50;

/// Record of a single error occurrence during chaos testing.
#[derive(Clone)]
struct ErrorRecord {
    operation: &'static str,
    error: String,
    resource: String,
}

/// Thread-safe metrics collection for tracking chaos test results.
/// Uses atomic counters for lock-free concurrent updates from multiple threads.
struct ChaosMetrics {
    // Warehouse operations
    create_warehouse: AtomicU64,
    delete_warehouse: AtomicU64,
    list_warehouses: AtomicU64,
    get_warehouse: AtomicU64,
    // Namespace operations
    create_namespace: AtomicU64,
    delete_namespace: AtomicU64,
    list_namespaces: AtomicU64,
    get_namespace: AtomicU64,
    namespace_exists: AtomicU64,
    // Table operations
    create_table: AtomicU64,
    delete_table: AtomicU64,
    rename_table: AtomicU64,
    load_table: AtomicU64,
    list_tables: AtomicU64,
    table_exists: AtomicU64,
    commit_table: AtomicU64,
    register_table: AtomicU64,
    // Multi-table operations
    commit_multi_tx: AtomicU64,
    // Config & Metrics
    get_config: AtomicU64,
    table_metrics: AtomicU64,
    // Namespace property operations
    update_namespace_properties: AtomicU64,
    // Credentials operations
    load_table_credentials: AtomicU64,
    // View operations
    create_view: AtomicU64,
    list_views: AtomicU64,
    load_view: AtomicU64,
    replace_view: AtomicU64,
    drop_view: AtomicU64,
    view_exists: AtomicU64,
    rename_view: AtomicU64,
    // Scan planning operations (V3)
    plan_table_scan: AtomicU64,
    fetch_planning_result: AtomicU64,
    fetch_scan_tasks: AtomicU64,
    cancel_planning: AtomicU64,
    // Error tracking
    total_errors: AtomicU64,
    deadlock_warnings: AtomicU64,
    recent_errors: Mutex<Vec<ErrorRecord>>,
    start_time: Instant,
}

impl ChaosMetrics {
    fn new() -> Self {
        Self {
            // Warehouse operations
            create_warehouse: AtomicU64::new(0),
            delete_warehouse: AtomicU64::new(0),
            list_warehouses: AtomicU64::new(0),
            get_warehouse: AtomicU64::new(0),
            // Namespace operations
            create_namespace: AtomicU64::new(0),
            delete_namespace: AtomicU64::new(0),
            list_namespaces: AtomicU64::new(0),
            get_namespace: AtomicU64::new(0),
            namespace_exists: AtomicU64::new(0),
            // Table operations
            create_table: AtomicU64::new(0),
            delete_table: AtomicU64::new(0),
            rename_table: AtomicU64::new(0),
            load_table: AtomicU64::new(0),
            list_tables: AtomicU64::new(0),
            table_exists: AtomicU64::new(0),
            commit_table: AtomicU64::new(0),
            register_table: AtomicU64::new(0),
            // Multi-table operations
            commit_multi_tx: AtomicU64::new(0),
            // Config & Metrics
            get_config: AtomicU64::new(0),
            table_metrics: AtomicU64::new(0),
            // Namespace property operations
            update_namespace_properties: AtomicU64::new(0),
            // Credentials operations
            load_table_credentials: AtomicU64::new(0),
            // View operations
            create_view: AtomicU64::new(0),
            list_views: AtomicU64::new(0),
            load_view: AtomicU64::new(0),
            replace_view: AtomicU64::new(0),
            drop_view: AtomicU64::new(0),
            view_exists: AtomicU64::new(0),
            rename_view: AtomicU64::new(0),
            // Scan planning operations (V3)
            plan_table_scan: AtomicU64::new(0),
            fetch_planning_result: AtomicU64::new(0),
            fetch_scan_tasks: AtomicU64::new(0),
            cancel_planning: AtomicU64::new(0),
            // Error tracking
            total_errors: AtomicU64::new(0),
            deadlock_warnings: AtomicU64::new(0),
            recent_errors: Mutex::new(Vec::new()),
            start_time: Instant::now(),
        }
    }

    fn record_error(&self, operation: &'static str, error: String, resource: String) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        let mut errors = self.recent_errors.lock().unwrap();
        if errors.len() >= MAX_RECENT_ERRORS {
            errors.remove(0);
        }
        errors.push(ErrorRecord {
            operation,
            error,
            resource,
        });
    }

    fn print_recent_errors(&self) {
        let errors = self.recent_errors.lock().unwrap();
        if errors.is_empty() {
            println!("  No errors recorded");
            return;
        }

        // Group errors by type
        let mut error_counts: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        for err in errors.iter() {
            let key = format!("{}: {}", err.operation, err.error);
            *error_counts.entry(key).or_insert(0) += 1;
        }

        println!("\nRecent Error Types (last {} errors):", errors.len());
        let mut sorted: Vec<_> = error_counts.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (error_type, count) in sorted.iter().take(15) {
            println!("  [{:>3}x] {}", count, error_type);
        }

        println!("\nLast 10 Errors:");
        for err in errors.iter().rev().take(10) {
            println!("  [{}] {} -> {}", err.operation, err.resource, err.error);
        }
    }

    fn total_ops(&self) -> u64 {
        // Warehouse operations
        self.create_warehouse.load(Ordering::Relaxed)
            + self.delete_warehouse.load(Ordering::Relaxed)
            + self.list_warehouses.load(Ordering::Relaxed)
            + self.get_warehouse.load(Ordering::Relaxed)
            // Namespace operations
            + self.create_namespace.load(Ordering::Relaxed)
            + self.delete_namespace.load(Ordering::Relaxed)
            + self.list_namespaces.load(Ordering::Relaxed)
            + self.get_namespace.load(Ordering::Relaxed)
            + self.namespace_exists.load(Ordering::Relaxed)
            // Table operations
            + self.create_table.load(Ordering::Relaxed)
            + self.delete_table.load(Ordering::Relaxed)
            + self.rename_table.load(Ordering::Relaxed)
            + self.load_table.load(Ordering::Relaxed)
            + self.list_tables.load(Ordering::Relaxed)
            + self.table_exists.load(Ordering::Relaxed)
            + self.commit_table.load(Ordering::Relaxed)
            + self.register_table.load(Ordering::Relaxed)
            // Multi-table operations
            + self.commit_multi_tx.load(Ordering::Relaxed)
            // Config & Metrics
            + self.get_config.load(Ordering::Relaxed)
            + self.table_metrics.load(Ordering::Relaxed)
            // Namespace property operations
            + self.update_namespace_properties.load(Ordering::Relaxed)
            // Credentials operations
            + self.load_table_credentials.load(Ordering::Relaxed)
            // View operations
            + self.create_view.load(Ordering::Relaxed)
            + self.list_views.load(Ordering::Relaxed)
            + self.load_view.load(Ordering::Relaxed)
            + self.replace_view.load(Ordering::Relaxed)
            + self.drop_view.load(Ordering::Relaxed)
            + self.view_exists.load(Ordering::Relaxed)
            + self.rename_view.load(Ordering::Relaxed)
            // Scan planning operations (V3)
            + self.plan_table_scan.load(Ordering::Relaxed)
            + self.fetch_planning_result.load(Ordering::Relaxed)
            + self.fetch_scan_tasks.load(Ordering::Relaxed)
            + self.cancel_planning.load(Ordering::Relaxed)
    }

    fn record(&self, op: ChaosOperation) {
        match op {
            // Warehouse operations
            ChaosOperation::CreateWarehouse => {
                self.create_warehouse.fetch_add(1, Ordering::Relaxed)
            }
            ChaosOperation::DeleteWarehouse => {
                self.delete_warehouse.fetch_add(1, Ordering::Relaxed)
            }
            ChaosOperation::ListWarehouses => self.list_warehouses.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::GetWarehouse => self.get_warehouse.fetch_add(1, Ordering::Relaxed),
            // Namespace operations
            ChaosOperation::CreateNamespace => {
                self.create_namespace.fetch_add(1, Ordering::Relaxed)
            }
            ChaosOperation::DeleteNamespace => {
                self.delete_namespace.fetch_add(1, Ordering::Relaxed)
            }
            ChaosOperation::ListNamespaces => self.list_namespaces.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::GetNamespace => self.get_namespace.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::NamespaceExists => {
                self.namespace_exists.fetch_add(1, Ordering::Relaxed)
            }
            // Table operations
            ChaosOperation::CreateTable => self.create_table.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::DeleteTable => self.delete_table.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::RenameTable => self.rename_table.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::LoadTable => self.load_table.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::ListTables => self.list_tables.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::TableExists => self.table_exists.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::CommitTable => self.commit_table.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::RegisterTable => self.register_table.fetch_add(1, Ordering::Relaxed),
            // Multi-table operations
            ChaosOperation::CommitMultiTableTransaction => {
                self.commit_multi_tx.fetch_add(1, Ordering::Relaxed)
            }
            // Config & Metrics
            ChaosOperation::GetConfig => self.get_config.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::TableMetrics => self.table_metrics.fetch_add(1, Ordering::Relaxed),
            // Namespace property operations
            ChaosOperation::UpdateNamespaceProperties => self
                .update_namespace_properties
                .fetch_add(1, Ordering::Relaxed),
            // Credentials operations
            ChaosOperation::LoadTableCredentials => {
                self.load_table_credentials.fetch_add(1, Ordering::Relaxed)
            }
            // View operations
            ChaosOperation::CreateView => self.create_view.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::ListViews => self.list_views.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::LoadView => self.load_view.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::ReplaceView => self.replace_view.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::DropView => self.drop_view.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::ViewExists => self.view_exists.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::RenameView => self.rename_view.fetch_add(1, Ordering::Relaxed),
            // Scan planning operations (V3)
            ChaosOperation::PlanTableScan => self.plan_table_scan.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::FetchPlanningResult => {
                self.fetch_planning_result.fetch_add(1, Ordering::Relaxed)
            }
            ChaosOperation::FetchScanTasks => self.fetch_scan_tasks.fetch_add(1, Ordering::Relaxed),
            ChaosOperation::CancelPlanning => self.cancel_planning.fetch_add(1, Ordering::Relaxed),
        };
    }

    fn print_progress(&self) {
        let elapsed = self.start_time.elapsed().as_secs();
        let total = self.total_ops();
        let ops_per_sec = if elapsed > 0 {
            total as f64 / elapsed as f64
        } else {
            0.0
        };

        println!("\n[{:>3}s] Chaos Progress:", elapsed);
        println!("  Total operations:   {}", total);
        println!("  Ops/sec:            {:.1}", ops_per_sec);
        // Warehouse operations
        println!(
            "  Create warehouse:   {}",
            self.create_warehouse.load(Ordering::Relaxed)
        );
        println!(
            "  Delete warehouse:   {}",
            self.delete_warehouse.load(Ordering::Relaxed)
        );
        println!(
            "  List warehouses:    {}",
            self.list_warehouses.load(Ordering::Relaxed)
        );
        println!(
            "  Get warehouse:      {}",
            self.get_warehouse.load(Ordering::Relaxed)
        );
        // Namespace operations
        println!(
            "  Create namespace:   {}",
            self.create_namespace.load(Ordering::Relaxed)
        );
        println!(
            "  Delete namespace:   {}",
            self.delete_namespace.load(Ordering::Relaxed)
        );
        println!(
            "  List namespaces:    {}",
            self.list_namespaces.load(Ordering::Relaxed)
        );
        println!(
            "  Get namespace:      {}",
            self.get_namespace.load(Ordering::Relaxed)
        );
        println!(
            "  Namespace exists:   {}",
            self.namespace_exists.load(Ordering::Relaxed)
        );
        // Table operations
        println!(
            "  Create table:       {}",
            self.create_table.load(Ordering::Relaxed)
        );
        println!(
            "  Delete table:       {}",
            self.delete_table.load(Ordering::Relaxed)
        );
        println!(
            "  Rename table:       {}",
            self.rename_table.load(Ordering::Relaxed)
        );
        println!(
            "  Load table:         {}",
            self.load_table.load(Ordering::Relaxed)
        );
        println!(
            "  List tables:        {}",
            self.list_tables.load(Ordering::Relaxed)
        );
        println!(
            "  Table exists:       {}",
            self.table_exists.load(Ordering::Relaxed)
        );
        println!(
            "  Commit table:       {}",
            self.commit_table.load(Ordering::Relaxed)
        );
        println!(
            "  Register table:     {}",
            self.register_table.load(Ordering::Relaxed)
        );
        // Multi-table operations
        println!(
            "  Commit multi-tx:    {}",
            self.commit_multi_tx.load(Ordering::Relaxed)
        );
        // Config & Metrics
        println!(
            "  Get config:         {}",
            self.get_config.load(Ordering::Relaxed)
        );
        println!(
            "  Table metrics:      {}",
            self.table_metrics.load(Ordering::Relaxed)
        );
        // Namespace property operations
        println!(
            "  Update ns props:    {}",
            self.update_namespace_properties.load(Ordering::Relaxed)
        );
        // Credentials operations
        println!(
            "  Load table creds:   {}",
            self.load_table_credentials.load(Ordering::Relaxed)
        );
        // View operations
        println!(
            "  Create view:        {}",
            self.create_view.load(Ordering::Relaxed)
        );
        println!(
            "  List views:         {}",
            self.list_views.load(Ordering::Relaxed)
        );
        println!(
            "  Load view:          {}",
            self.load_view.load(Ordering::Relaxed)
        );
        println!(
            "  Replace view:       {}",
            self.replace_view.load(Ordering::Relaxed)
        );
        println!(
            "  Drop view:          {}",
            self.drop_view.load(Ordering::Relaxed)
        );
        println!(
            "  View exists:        {}",
            self.view_exists.load(Ordering::Relaxed)
        );
        println!(
            "  Rename view:        {}",
            self.rename_view.load(Ordering::Relaxed)
        );
        // Scan planning operations (V3)
        println!(
            "  Plan table scan:    {}",
            self.plan_table_scan.load(Ordering::Relaxed)
        );
        println!(
            "  Fetch plan result:  {}",
            self.fetch_planning_result.load(Ordering::Relaxed)
        );
        println!(
            "  Fetch scan tasks:   {}",
            self.fetch_scan_tasks.load(Ordering::Relaxed)
        );
        println!(
            "  Cancel planning:    {}",
            self.cancel_planning.load(Ordering::Relaxed)
        );
        // Errors
        println!(
            "  Total errors:       {}",
            self.total_errors.load(Ordering::Relaxed)
        );
        println!(
            "  Deadlock warnings:  {}",
            self.deadlock_warnings.load(Ordering::Relaxed)
        );
    }

    fn print_summary(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let total = self.total_ops();
        let errors = self.total_errors.load(Ordering::Relaxed);
        let error_rate = if total > 0 {
            (errors as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        println!("\n{}", "=".repeat(60));
        println!("=== Final Chaos Statistics ===");
        println!("{}", "=".repeat(60));
        println!("Duration:             {:.1} minutes", elapsed / 60.0);
        println!("Total operations:     {}", total);
        println!("Operations/sec:       {:.2}", total as f64 / elapsed);
        println!("\nWarehouse Operations (4):");
        println!(
            "  Create warehouse:   {}",
            self.create_warehouse.load(Ordering::Relaxed)
        );
        println!(
            "  Delete warehouse:   {}",
            self.delete_warehouse.load(Ordering::Relaxed)
        );
        println!(
            "  List warehouses:    {}",
            self.list_warehouses.load(Ordering::Relaxed)
        );
        println!(
            "  Get warehouse:      {}",
            self.get_warehouse.load(Ordering::Relaxed)
        );
        println!("\nNamespace Operations (5):");
        println!(
            "  Create namespace:   {}",
            self.create_namespace.load(Ordering::Relaxed)
        );
        println!(
            "  Delete namespace:   {}",
            self.delete_namespace.load(Ordering::Relaxed)
        );
        println!(
            "  List namespaces:    {}",
            self.list_namespaces.load(Ordering::Relaxed)
        );
        println!(
            "  Get namespace:      {}",
            self.get_namespace.load(Ordering::Relaxed)
        );
        println!(
            "  Namespace exists:   {}",
            self.namespace_exists.load(Ordering::Relaxed)
        );
        println!("\nTable Operations (8):");
        println!(
            "  Create table:       {}",
            self.create_table.load(Ordering::Relaxed)
        );
        println!(
            "  Delete table:       {}",
            self.delete_table.load(Ordering::Relaxed)
        );
        println!(
            "  Rename table:       {}",
            self.rename_table.load(Ordering::Relaxed)
        );
        println!(
            "  Load table:         {}",
            self.load_table.load(Ordering::Relaxed)
        );
        println!(
            "  List tables:        {}",
            self.list_tables.load(Ordering::Relaxed)
        );
        println!(
            "  Table exists:       {}",
            self.table_exists.load(Ordering::Relaxed)
        );
        println!(
            "  Commit table:       {}",
            self.commit_table.load(Ordering::Relaxed)
        );
        println!(
            "  Register table:     {}",
            self.register_table.load(Ordering::Relaxed)
        );
        println!("\nMulti-Table Operations (1):");
        println!(
            "  Commit multi-tx:    {}",
            self.commit_multi_tx.load(Ordering::Relaxed)
        );
        println!("\nConfig & Metrics (2):");
        println!(
            "  Get config:         {}",
            self.get_config.load(Ordering::Relaxed)
        );
        println!(
            "  Table metrics:      {}",
            self.table_metrics.load(Ordering::Relaxed)
        );
        println!("\nNamespace Property Operations (1):");
        println!(
            "  Update ns props:    {}",
            self.update_namespace_properties.load(Ordering::Relaxed)
        );
        println!("\nCredentials Operations (1):");
        println!(
            "  Load table creds:   {}",
            self.load_table_credentials.load(Ordering::Relaxed)
        );
        println!("\nView Operations (7):");
        println!(
            "  Create view:        {}",
            self.create_view.load(Ordering::Relaxed)
        );
        println!(
            "  List views:         {}",
            self.list_views.load(Ordering::Relaxed)
        );
        println!(
            "  Load view:          {}",
            self.load_view.load(Ordering::Relaxed)
        );
        println!(
            "  Replace view:       {}",
            self.replace_view.load(Ordering::Relaxed)
        );
        println!(
            "  Drop view:          {}",
            self.drop_view.load(Ordering::Relaxed)
        );
        println!(
            "  View exists:        {}",
            self.view_exists.load(Ordering::Relaxed)
        );
        println!(
            "  Rename view:        {}",
            self.rename_view.load(Ordering::Relaxed)
        );
        println!("\nScan Planning Operations (4) - V3 APIs:");
        println!(
            "  Plan table scan:    {}",
            self.plan_table_scan.load(Ordering::Relaxed)
        );
        println!(
            "  Fetch plan result:  {}",
            self.fetch_planning_result.load(Ordering::Relaxed)
        );
        println!(
            "  Fetch scan tasks:   {}",
            self.fetch_scan_tasks.load(Ordering::Relaxed)
        );
        println!(
            "  Cancel planning:    {}",
            self.cancel_planning.load(Ordering::Relaxed)
        );
        println!("\nErrors and Issues:");
        println!("  Total errors:       {}", errors);
        println!("  Error rate:         {:.2}%", error_rate);
        println!(
            "  Deadlock warnings:  {}",
            self.deadlock_warnings.load(Ordering::Relaxed)
        );

        self.print_recent_errors();
    }
}

/// Shared state tracking all known resources across chaos threads.
/// Thread-safe via Arc<Mutex<...>> wrappers for concurrent access.
#[derive(Clone)]
struct SharedState {
    warehouses: Arc<Mutex<HashSet<String>>>,
    namespaces: Arc<Mutex<HashSet<NamespaceId>>>,
    tables: Arc<Mutex<HashSet<TableId>>>,
    views: Arc<Mutex<HashSet<ViewId>>>,
    plan_ids: Arc<Mutex<HashSet<PlanId>>>,
    counter: Arc<AtomicU64>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            warehouses: Arc::new(Mutex::new(HashSet::new())),
            namespaces: Arc::new(Mutex::new(HashSet::new())),
            tables: Arc::new(Mutex::new(HashSet::new())),
            views: Arc::new(Mutex::new(HashSet::new())),
            plan_ids: Arc::new(Mutex::new(HashSet::new())),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    fn add_warehouse(&self, name: &str) {
        self.warehouses.lock().unwrap().insert(name.to_string());
    }

    fn remove_warehouse(&self, name: &str) {
        self.warehouses.lock().unwrap().remove(name);
    }

    fn random_warehouse(&self) -> Option<String> {
        let guard = self.warehouses.lock().unwrap();
        if guard.is_empty() {
            None
        } else {
            let idx = rand::rng().random_range(0..guard.len());
            guard.iter().nth(idx).cloned()
        }
    }

    fn add_namespace(&self, warehouse: &str, namespace: Vec<String>) {
        self.namespaces
            .lock()
            .unwrap()
            .insert((warehouse.to_string(), namespace));
    }

    fn remove_namespace(&self, warehouse: &str, namespace: &[String]) {
        self.namespaces
            .lock()
            .unwrap()
            .remove(&(warehouse.to_string(), namespace.to_vec()));
    }

    fn random_namespace(&self) -> Option<(String, Vec<String>)> {
        let guard = self.namespaces.lock().unwrap();
        if guard.is_empty() {
            None
        } else {
            let idx = rand::rng().random_range(0..guard.len());
            guard.iter().nth(idx).cloned()
        }
    }

    fn add_table(&self, warehouse: &str, namespace: Vec<String>, table: &str) {
        self.tables
            .lock()
            .unwrap()
            .insert((warehouse.to_string(), namespace, table.to_string()));
    }

    fn remove_table(&self, warehouse: &str, namespace: &[String], table: &str) {
        self.tables.lock().unwrap().remove(&(
            warehouse.to_string(),
            namespace.to_vec(),
            table.to_string(),
        ));
    }

    fn random_table(&self) -> Option<(String, Vec<String>, String)> {
        let guard = self.tables.lock().unwrap();
        if guard.is_empty() {
            None
        } else {
            let idx = rand::rng().random_range(0..guard.len());
            guard.iter().nth(idx).cloned()
        }
    }

    fn add_view(&self, warehouse: &str, namespace: Vec<String>, view: &str) {
        self.views
            .lock()
            .unwrap()
            .insert((warehouse.to_string(), namespace, view.to_string()));
    }

    fn remove_view(&self, warehouse: &str, namespace: &[String], view: &str) {
        self.views.lock().unwrap().remove(&(
            warehouse.to_string(),
            namespace.to_vec(),
            view.to_string(),
        ));
    }

    fn random_view(&self) -> Option<(String, Vec<String>, String)> {
        let guard = self.views.lock().unwrap();
        if guard.is_empty() {
            None
        } else {
            let idx = rand::rng().random_range(0..guard.len());
            guard.iter().nth(idx).cloned()
        }
    }

    fn add_plan_id(&self, warehouse: &str, namespace: Vec<String>, table: &str, plan_id: &str) {
        self.plan_ids.lock().unwrap().insert((
            warehouse.to_string(),
            namespace,
            table.to_string(),
            plan_id.to_string(),
        ));
    }

    fn remove_plan_id(&self, warehouse: &str, namespace: &[String], table: &str, plan_id: &str) {
        self.plan_ids.lock().unwrap().remove(&(
            warehouse.to_string(),
            namespace.to_vec(),
            table.to_string(),
            plan_id.to_string(),
        ));
    }

    fn random_plan_id(&self) -> Option<(String, Vec<String>, String, String)> {
        let guard = self.plan_ids.lock().unwrap();
        if guard.is_empty() {
            None
        } else {
            let idx = rand::rng().random_range(0..guard.len());
            guard.iter().nth(idx).cloned()
        }
    }
}

/// Creates a simple Iceberg schema with `id` (long) and `data` (string) fields.
/// Used for creating tables and views during chaos testing.
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
                doc: Some("Data".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    }
}

/// Worker thread that continuously executes random chaos operations until stopped.
/// Each iteration: picks a random operation, executes it, records metrics, and sleeps.
async fn chaos_thread(
    tables: TablesClient,
    state: SharedState,
    metrics: Arc<ChaosMetrics>,
    stop_signal: Arc<AtomicBool>,
    thread_id: usize,
) {
    let schema = create_test_schema();
    let mut local_ops = 0u64;
    let thread_start = Instant::now();

    while !stop_signal.load(Ordering::Relaxed) {
        let operation = ChaosOperation::random();
        let op_start = Instant::now();
        let mut success = false;

        match operation {
            ChaosOperation::CreateWarehouse => {
                let wh_name = format!("chaos-wh-{}", state.next_id());
                if let Ok(warehouse) = WarehouseName::try_from(wh_name.as_str()) {
                    match tables.create_warehouse(warehouse).build().send().await {
                        Ok(_) => {
                            state.add_warehouse(&wh_name);
                            success = true;
                        }
                        Err(e) => {
                            metrics.record_error(operation.name(), e.to_string(), wh_name);
                        }
                    }
                }
            }

            ChaosOperation::DeleteWarehouse => {
                if let Some(wh) = state.random_warehouse()
                    && let Ok(warehouse) = WarehouseName::try_from(wh.as_str())
                {
                    match tables.delete_warehouse(warehouse).build().send().await {
                        Ok(_) => {
                            state.remove_warehouse(&wh);
                            success = true;
                        }
                        Err(e) => {
                            metrics.record_error(operation.name(), e.to_string(), wh);
                        }
                    }
                }
            }

            ChaosOperation::CreateNamespace => {
                if let Some(wh) = state.random_warehouse() {
                    let ns_name = format!("ns{}", state.next_id());
                    let namespace = vec![ns_name.clone()];
                    let resource = format!("{}/{}", wh, ns_name);
                    if let (Ok(warehouse), Ok(ns)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(namespace.clone()),
                    ) {
                        match tables.create_namespace(warehouse, ns).build().send().await {
                            Ok(_) => {
                                state.add_namespace(&wh, namespace);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::DeleteNamespace => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let resource = format!("{}/{}", wh, ns.join("/"));
                    if let (Ok(warehouse), Ok(namespace)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                    ) {
                        match tables
                            .delete_namespace(warehouse, namespace)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.remove_namespace(&wh, &ns);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::CreateTable => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let tbl_name = format!("tbl{}", state.next_id());
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl_name);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl_name.as_str()),
                    ) {
                        match tables
                            .create_table(warehouse, namespace, table_name, schema.clone())
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.add_table(&wh, ns, &tbl_name);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::DeleteTable => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .delete_table(warehouse, namespace, table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.remove_table(&wh, &ns, &tbl);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::RenameTable => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let new_name = format!("renamed{}", state.next_id());
                    let resource = format!("{}/{}/{} -> {}", wh, ns.join("/"), tbl, new_name);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name), Ok(new_table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                        TableName::try_from(new_name.as_str()),
                    ) {
                        match tables
                            .rename_table(
                                warehouse,
                                namespace.clone(),
                                table_name,
                                namespace,
                                new_table_name,
                            )
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.remove_table(&wh, &ns, &tbl);
                                state.add_table(&wh, ns, &new_name);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::LoadTable => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .load_table(warehouse, namespace, table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::ListNamespaces => {
                if let Some(wh) = state.random_warehouse()
                    && let Ok(warehouse) = WarehouseName::try_from(wh.as_str())
                {
                    match tables.list_namespaces(warehouse).build().send().await {
                        Ok(_) => {
                            success = true;
                        }
                        Err(e) => {
                            metrics.record_error(operation.name(), e.to_string(), wh);
                        }
                    }
                }
            }

            ChaosOperation::ListTables => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let resource = format!("{}/{}", wh, ns.join("/"));
                    if let (Ok(warehouse), Ok(namespace)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                    ) {
                        match tables
                            .list_tables(warehouse, namespace)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::GetWarehouse => {
                if let Some(wh) = state.random_warehouse()
                    && let Ok(warehouse) = WarehouseName::try_from(wh.as_str())
                {
                    match tables.get_warehouse(warehouse).build().send().await {
                        Ok(_) => {
                            success = true;
                        }
                        Err(e) => {
                            metrics.record_error(operation.name(), e.to_string(), wh);
                        }
                    }
                }
            }

            ChaosOperation::ListWarehouses => match tables.list_warehouses().build().send().await {
                Ok(_) => {
                    success = true;
                }
                Err(e) => {
                    metrics.record_error(operation.name(), e.to_string(), "all".to_string());
                }
            },

            ChaosOperation::GetNamespace => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let resource = format!("{}/{}", wh, ns.join("/"));
                    if let (Ok(warehouse), Ok(namespace)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                    ) {
                        match tables
                            .get_namespace(warehouse, namespace)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::NamespaceExists => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let resource = format!("{}/{}", wh, ns.join("/"));
                    if let (Ok(warehouse), Ok(namespace)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                    ) {
                        match tables
                            .namespace_exists(warehouse, namespace)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::TableExists => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .table_exists(warehouse, namespace, table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::CommitTable => {
                // CommitTable requires loading the table first to get current metadata
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        // First load the table to get current metadata
                        match tables
                            .load_table(warehouse.clone(), namespace.clone(), table_name.clone())
                            .build()
                            .send()
                            .await
                        {
                            Ok(load_resp) => {
                                // Get the table result which contains metadata
                                match load_resp.table_result() {
                                    Ok(result) => {
                                        // Now try to commit with the current metadata (no-op update)
                                        match tables
                                            .commit_table(
                                                warehouse,
                                                namespace,
                                                table_name,
                                                result.metadata.clone(),
                                            )
                                            .build()
                                            .send()
                                            .await
                                        {
                                            Ok(_) => {
                                                success = true;
                                            }
                                            Err(e) => {
                                                metrics.record_error(
                                                    operation.name(),
                                                    e.to_string(),
                                                    resource,
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        metrics.record_error(
                                            operation.name(),
                                            format!("parse failed: {}", e),
                                            resource,
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                // LoadTable failed, record as CommitTable error since that's the operation
                                metrics.record_error(
                                    operation.name(),
                                    format!("load failed: {}", e),
                                    resource,
                                );
                            }
                        }
                    }
                }
            }

            ChaosOperation::RegisterTable => {
                // RegisterTable requires a metadata location, which we can get from an existing table
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        // First load the table to get its metadata location
                        match tables
                            .load_table(warehouse.clone(), namespace.clone(), table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(load_resp) => {
                                if let Ok(result) = load_resp.table_result() {
                                    // Only proceed if we have a metadata location
                                    if let Some(metadata_loc) = result.metadata_location {
                                        // Create a new table name for registration
                                        let new_tbl_name = format!("reg{}", state.next_id());
                                        let new_resource =
                                            format!("{}/{}/{}", wh, ns.join("/"), new_tbl_name);
                                        if let (Ok(new_table_name), Ok(metadata_location)) = (
                                            TableName::try_from(new_tbl_name.as_str()),
                                            MetadataLocation::new(&metadata_loc),
                                        ) {
                                            // Try to register a table with the same metadata location
                                            match tables
                                                .register_table(
                                                    warehouse,
                                                    namespace.clone(),
                                                    new_table_name,
                                                    metadata_location,
                                                )
                                                .build()
                                                .send()
                                                .await
                                            {
                                                Ok(_) => {
                                                    state.add_table(&wh, ns, &new_tbl_name);
                                                    success = true;
                                                }
                                                Err(e) => {
                                                    metrics.record_error(
                                                        operation.name(),
                                                        e.to_string(),
                                                        new_resource,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                metrics.record_error(
                                    operation.name(),
                                    format!("load failed: {}", e),
                                    resource,
                                );
                            }
                        }
                    }
                }
            }

            ChaosOperation::CommitMultiTableTransaction => {
                // Get two random tables from the same warehouse for a multi-table transaction
                if let (Some((wh1, ns1, tbl1)), Some((wh2, ns2, tbl2))) =
                    (state.random_table(), state.random_table())
                {
                    // Only proceed if both tables are in the same warehouse
                    if wh1 == wh2 {
                        let resource = format!(
                            "{}:[{}/{}],[{}/{}]",
                            wh1,
                            ns1.join("/"),
                            tbl1,
                            ns2.join("/"),
                            tbl2
                        );
                        if let (
                            Ok(warehouse),
                            Ok(namespace1),
                            Ok(table_name1),
                            Ok(namespace2),
                            Ok(table_name2),
                        ) = (
                            WarehouseName::try_from(wh1.as_str()),
                            Namespace::try_from(ns1.clone()),
                            TableName::try_from(tbl1.as_str()),
                            Namespace::try_from(ns2.clone()),
                            TableName::try_from(tbl2.as_str()),
                        ) {
                            // Load both tables to get their metadata
                            let load1 = tables
                                .load_table(
                                    warehouse.clone(),
                                    namespace1.clone(),
                                    table_name1.clone(),
                                )
                                .build()
                                .send()
                                .await;
                            let load2 = tables
                                .load_table(
                                    warehouse.clone(),
                                    namespace2.clone(),
                                    table_name2.clone(),
                                )
                                .build()
                                .send()
                                .await;

                            match (load1, load2) {
                                (Ok(resp1), Ok(resp2)) => {
                                    if let (Ok(_), Ok(_)) =
                                        (resp1.table_result(), resp2.table_result())
                                    {
                                        // Create table changes (no-op updates)
                                        let changes = vec![
                                            TableChange {
                                                identifier: TableIdentifier {
                                                    namespace: namespace1.clone(),
                                                    name: table_name1.clone(),
                                                },
                                                requirements: vec![],
                                                updates: vec![],
                                            },
                                            TableChange {
                                                identifier: TableIdentifier {
                                                    namespace: namespace2.clone(),
                                                    name: table_name2.clone(),
                                                },
                                                requirements: vec![],
                                                updates: vec![],
                                            },
                                        ];
                                        match tables
                                            .commit_multi_table_transaction(warehouse, changes)
                                            .build()
                                            .send()
                                            .await
                                        {
                                            Ok(_) => {
                                                success = true;
                                            }
                                            Err(e) => {
                                                metrics.record_error(
                                                    operation.name(),
                                                    e.to_string(),
                                                    resource,
                                                );
                                            }
                                        }
                                    }
                                }
                                (Err(e), _) | (_, Err(e)) => {
                                    metrics.record_error(
                                        operation.name(),
                                        format!("load failed: {}", e),
                                        resource,
                                    );
                                }
                            }
                        }
                    }
                }
            }

            ChaosOperation::GetConfig => {
                if let Some(wh) = state.random_warehouse()
                    && let Ok(warehouse) = WarehouseName::try_from(wh.as_str())
                {
                    match tables.get_config(warehouse).build().send().await {
                        Ok(_) => {
                            success = true;
                        }
                        Err(e) => {
                            metrics.record_error(operation.name(), e.to_string(), wh);
                        }
                    }
                }
            }

            ChaosOperation::TableMetrics => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .table_metrics(warehouse, namespace, table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::UpdateNamespaceProperties => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let resource = format!("{}/{}", wh, ns.join("/"));
                    if let (Ok(warehouse), Ok(namespace)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                    ) {
                        let mut updates = HashMap::new();
                        updates.insert(
                            format!("chaos-prop-{}", state.next_id()),
                            format!("value-{}", state.next_id()),
                        );
                        match tables
                            .update_namespace_properties(warehouse, namespace)
                            .updates(updates)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::LoadTableCredentials => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .load_table_credentials(warehouse, namespace, table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::CreateView => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let view_name = format!("view{}", state.next_id());
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), view_name);
                    if let (Ok(warehouse), Ok(namespace), Ok(view_name_wrapped)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        ViewName::try_from(view_name.as_str()),
                    ) {
                        let Ok(view_sql) = ViewSql::new("SELECT 1") else {
                            continue;
                        };
                        match tables
                            .create_view(
                                warehouse,
                                namespace.clone(),
                                view_name_wrapped,
                                schema.clone(),
                                view_sql,
                            )
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.add_view(&wh, ns, &view_name);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::ListViews => {
                if let Some((wh, ns)) = state.random_namespace() {
                    let resource = format!("{}/{}", wh, ns.join("/"));
                    if let (Ok(warehouse), Ok(namespace)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                    ) {
                        match tables.list_views(warehouse, namespace).build().send().await {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::LoadView => {
                if let Some((wh, ns, view)) = state.random_view() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), view);
                    if let (Ok(warehouse), Ok(namespace), Ok(view_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        ViewName::try_from(view.as_str()),
                    ) {
                        match tables
                            .load_view(warehouse, namespace, view_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::ReplaceView => {
                if let Some((wh, ns, view)) = state.random_view() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), view);
                    if let (Ok(warehouse), Ok(namespace), Ok(view_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        ViewName::try_from(view.as_str()),
                    ) {
                        // ReplaceView requires updates, we'll skip complex updates for chaos test
                        match tables
                            .replace_view(warehouse, namespace, view_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::DropView => {
                if let Some((wh, ns, view)) = state.random_view() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), view);
                    if let (Ok(warehouse), Ok(namespace), Ok(view_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        ViewName::try_from(view.as_str()),
                    ) {
                        match tables
                            .drop_view(warehouse, namespace.clone(), view_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.remove_view(&wh, &ns, &view);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::ViewExists => {
                if let Some((wh, ns, view)) = state.random_view() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), view);
                    if let (Ok(warehouse), Ok(namespace), Ok(view_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        ViewName::try_from(view.as_str()),
                    ) {
                        match tables
                            .view_exists(warehouse, namespace, view_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::RenameView => {
                if let Some((wh, ns, view)) = state.random_view() {
                    let new_name = format!("renamed_view{}", state.next_id());
                    let resource = format!("{}/{}/{} -> {}", wh, ns.join("/"), view, new_name);
                    if let (Ok(warehouse), Ok(namespace), Ok(view_name), Ok(new_view_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        ViewName::try_from(view.as_str()),
                        ViewName::try_from(new_name.as_str()),
                    ) {
                        match tables
                            .rename_view(
                                warehouse,
                                namespace.clone(),
                                view_name,
                                namespace.clone(),
                                new_view_name,
                            )
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                state.remove_view(&wh, &ns, &view);
                                state.add_view(&wh, ns, &new_name);
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }

            ChaosOperation::PlanTableScan => {
                if let Some((wh, ns, tbl)) = state.random_table() {
                    let resource = format!("{}/{}/{}", wh, ns.join("/"), tbl);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .plan_table_scan(warehouse, namespace.clone(), table_name)
                            .build()
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                success = true;
                                if let Ok(result) = resp.result()
                                    && let Some(plan_id) = result.plan_id
                                {
                                    state.add_plan_id(&wh, ns, &tbl, plan_id.as_str());
                                }
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }
            ChaosOperation::FetchPlanningResult => {
                if let Some((wh, ns, tbl, plan_id_str)) = state.random_plan_id() {
                    let resource = format!("{}/{}/{}/{}", wh, ns.join("/"), tbl, plan_id_str);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name), Ok(plan_id)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                        PlanIdType::new(&plan_id_str),
                    ) {
                        match tables
                            .fetch_planning_result(warehouse, namespace, table_name, plan_id)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }
            ChaosOperation::FetchScanTasks => {
                if let Some((wh, ns, tbl, plan_id)) = state.random_plan_id() {
                    let resource = format!("{}/{}/{}/{}", wh, ns.join("/"), tbl, plan_id);
                    let plan_task = serde_json::json!({ "plan-id": plan_id });
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                    ) {
                        match tables
                            .fetch_scan_tasks(warehouse, namespace, table_name, plan_task)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }
            ChaosOperation::CancelPlanning => {
                if let Some((wh, ns, tbl, plan_id_str)) = state.random_plan_id() {
                    let resource = format!("{}/{}/{}/{}", wh, ns.join("/"), tbl, plan_id_str);
                    if let (Ok(warehouse), Ok(namespace), Ok(table_name), Ok(plan_id)) = (
                        WarehouseName::try_from(wh.as_str()),
                        Namespace::try_from(ns.clone()),
                        TableName::try_from(tbl.as_str()),
                        PlanIdType::new(&plan_id_str),
                    ) {
                        match tables
                            .cancel_planning(warehouse, namespace.clone(), table_name, plan_id)
                            .build()
                            .send()
                            .await
                        {
                            Ok(_) => {
                                success = true;
                                state.remove_plan_id(&wh, &ns, &tbl, &plan_id_str);
                            }
                            Err(e) => {
                                metrics.record_error(operation.name(), e.to_string(), resource);
                            }
                        }
                    }
                }
            }
        }

        if success {
            metrics.record(operation);
        }

        let op_duration = op_start.elapsed();
        if op_duration > Duration::from_secs(5) {
            metrics.deadlock_warnings.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[Thread {}] Operation {} took {:.1}s (possible deadlock?)",
                thread_id,
                operation.name(),
                op_duration.as_secs_f64()
            );
        }

        local_ops += 1;

        let sleep_ms = rand::rng().random_range(MIN_SLEEP_MS..MAX_SLEEP_MS);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    let duration = thread_start.elapsed().as_secs_f64();
    println!(
        "[Thread {}] Completed {} operations in {:.1}s ({:.1} ops/s)",
        thread_id,
        local_ops,
        duration,
        local_ops as f64 / duration
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== S3 Tables Stress Test: STATE CHAOS ===\n");

    // Load configuration from environment
    let config = Config::from_env();

    println!("Configuration:");
    println!("  Initial warehouses:     {}", NUM_INITIAL_WAREHOUSES);
    println!(
        "  Initial namespaces/wh:  {}",
        NUM_INITIAL_NAMESPACES_PER_WH
    );
    println!("  Initial tables/ns:      {}", NUM_INITIAL_TABLES_PER_NS);
    println!("  Chaos threads:          {}", NUM_CHAOS_THREADS);
    println!(
        "  Test duration:          {} seconds ({:.1} minutes)",
        TEST_DURATION_SECS,
        TEST_DURATION_SECS as f64 / 60.0
    );
    println!(
        "  Operation interval:     {}-{}ms",
        MIN_SLEEP_MS, MAX_SLEEP_MS
    );
    println!();
    config.print();
    println!();

    let tables = config.create_client()?;
    println!("Client created with auth: {}\n", tables.auth_name());
    let schema = create_test_schema();
    let state = SharedState::new();

    println!("Step 1: Creating initial infrastructure...");

    for wh_idx in 0..NUM_INITIAL_WAREHOUSES {
        let wh_name = format!("chaos-init-wh-{}", wh_idx);
        if let Ok(warehouse) = WarehouseName::try_from(wh_name.as_str()) {
            match tables.create_warehouse(warehouse).build().send().await {
                Ok(_) => {
                    state.add_warehouse(&wh_name);
                    println!("  Created warehouse: {}", wh_name);
                }
                Err(_) => {
                    println!("  Warehouse may exist: {}", wh_name);
                    state.add_warehouse(&wh_name);
                }
            }

            for ns_idx in 0..NUM_INITIAL_NAMESPACES_PER_WH {
                let ns_name = format!("ns{}", ns_idx);
                let namespace = vec![ns_name.clone()];
                if let (Ok(warehouse), Ok(ns)) = (
                    WarehouseName::try_from(wh_name.as_str()),
                    Namespace::try_from(namespace.clone()),
                ) {
                    match tables
                        .create_namespace(warehouse.clone(), ns)
                        .build()
                        .send()
                        .await
                    {
                        Ok(_) => {
                            state.add_namespace(&wh_name, namespace.clone());
                        }
                        Err(_) => {
                            state.add_namespace(&wh_name, namespace.clone());
                        }
                    }

                    for tbl_idx in 0..NUM_INITIAL_TABLES_PER_NS {
                        let tbl_name = format!("tbl{}_{}", ns_idx, tbl_idx);
                        if let (Ok(warehouse), Ok(ns), Ok(table_name)) = (
                            WarehouseName::try_from(wh_name.as_str()),
                            Namespace::try_from(namespace.clone()),
                            TableName::try_from(tbl_name.as_str()),
                        ) {
                            match tables
                                .create_table(warehouse, ns, table_name, schema.clone())
                                .build()
                                .send()
                                .await
                            {
                                Ok(_) => {
                                    state.add_table(&wh_name, namespace.clone(), &tbl_name);
                                }
                                Err(_) => {
                                    state.add_table(&wh_name, namespace.clone(), &tbl_name);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let total_tables =
        NUM_INITIAL_WAREHOUSES * NUM_INITIAL_NAMESPACES_PER_WH * NUM_INITIAL_TABLES_PER_NS;
    println!(
        "  Created {} warehouses, {} namespaces, {} tables\n",
        NUM_INITIAL_WAREHOUSES,
        NUM_INITIAL_WAREHOUSES * NUM_INITIAL_NAMESPACES_PER_WH,
        total_tables
    );

    println!("Step 2: Starting chaos test...\n");
    let metrics = Arc::new(ChaosMetrics::new());
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();

    for thread_id in 0..NUM_CHAOS_THREADS {
        let tables_clone = config.create_client()?;
        let state_clone = state.clone();
        let metrics_clone = Arc::clone(&metrics);
        let stop_signal_clone = Arc::clone(&stop_signal);

        tasks.spawn(async move {
            chaos_thread(
                tables_clone,
                state_clone,
                metrics_clone,
                stop_signal_clone,
                thread_id,
            )
            .await;
        });
    }

    let progress_metrics = Arc::clone(&metrics);
    let progress_stop = Arc::clone(&stop_signal);
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        while !progress_stop.load(Ordering::Relaxed) {
            interval.tick().await;
            if !progress_stop.load(Ordering::Relaxed) {
                progress_metrics.print_progress();
            }
        }
    });

    println!(
        "CHAOS INITIATED - Running for {} seconds...\n",
        TEST_DURATION_SECS
    );
    tokio::time::sleep(Duration::from_secs(TEST_DURATION_SECS)).await;

    println!("\n\nStopping all threads...");
    stop_signal.store(true, Ordering::Relaxed);

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Thread panicked: {}", e);
        }
    }

    progress_handle.abort();

    println!("\n=== All Threads Stopped ===");
    metrics.print_summary();

    let total = metrics.total_ops();
    let errors = metrics.total_errors.load(Ordering::Relaxed);
    let deadlocks = metrics.deadlock_warnings.load(Ordering::Relaxed);

    if deadlocks == 0 && total > 0 && (errors as f64 / total as f64) < 0.5 {
        println!("\nSystem survived chaos test!");
    } else {
        println!("\nIssues detected during chaos test - review warnings above");
    }

    // Optional cleanup using the new delete_and_purge_warehouse API
    if env::var("CLEANUP_AFTER_TEST").is_ok() {
        println!("\nStep 3: Cleaning up warehouses using delete_and_purge_warehouse...");
        let warehouses: Vec<String> = state.warehouses.lock().unwrap().iter().cloned().collect();
        for wh in warehouses {
            print!("  Purging {}... ", wh);
            if let Ok(warehouse) = WarehouseName::try_from(wh.as_str()) {
                match tables.delete_and_purge_warehouse(warehouse).await {
                    Ok(_) => println!("done"),
                    Err(e) => println!("error: {}", e),
                }
            }
        }
        println!("Cleanup complete!");
    } else {
        println!("\nNote: Set CLEANUP_AFTER_TEST=1 to clean up warehouses after test");
    }

    Ok(())
}
