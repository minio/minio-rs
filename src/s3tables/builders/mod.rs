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

//! Argument builders for Tables API operations

// Warehouse operations
mod create_warehouse;
mod delete_warehouse;
mod get_warehouse;
mod list_warehouses;

pub use create_warehouse::{CreateWarehouse, CreateWarehouseBldr};
pub use delete_warehouse::{DeleteWarehouse, DeleteWarehouseBldr};
pub use get_warehouse::{GetWarehouse, GetWarehouseBldr};
pub use list_warehouses::{ListWarehouses, ListWarehousesBldr};

// Namespace operations
mod create_namespace;
mod delete_namespace;
mod get_namespace;
mod list_namespaces;
mod namespace_exists;

pub use create_namespace::{CreateNamespace, CreateNamespaceBldr};
pub use delete_namespace::{DeleteNamespace, DeleteNamespaceBldr};
pub use get_namespace::{GetNamespace, GetNamespaceBldr};
pub use list_namespaces::{ListNamespaces, ListNamespacesBldr};
pub use namespace_exists::{NamespaceExists, NamespaceExistsBldr};

// Table operations
mod commit_multi_table_transaction;
pub mod commit_table;
mod create_table;
mod delete_table;
mod list_tables;
mod load_table;
mod register_table;
mod rename_table;
mod table_exists;

pub use commit_multi_table_transaction::{
    CommitMultiTableTransaction, CommitMultiTableTransactionBldr, TableChange, TableIdentifier,
};
pub use commit_table::{CommitTable, CommitTableBldr, TableRequirement, TableUpdate};
pub use create_table::{CreateTable, CreateTableBldr};
pub use delete_table::{DeleteTable, DeleteTableBldr};
pub use list_tables::{ListTables, ListTablesBldr};
pub use load_table::{LoadTable, LoadTableBldr};
pub use register_table::{RegisterTable, RegisterTableBldr};
pub use rename_table::{RenameTable, RenameTableBldr};
pub use table_exists::{TableExists, TableExistsBldr};

// Configuration & Metrics
mod get_config;
mod table_metrics;

pub use get_config::{GetConfig, GetConfigBldr};
pub use table_metrics::{TableMetrics, TableMetricsBldr};
