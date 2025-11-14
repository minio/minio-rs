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

//! Response types for Tables API operations

// Warehouse operations
mod create_warehouse;
mod delete_warehouse;
mod get_warehouse;
mod list_warehouses;

pub use create_warehouse::CreateWarehouseResponse;
pub use delete_warehouse::DeleteWarehouseResponse;
pub use get_warehouse::GetWarehouseResponse;
pub use list_warehouses::ListWarehousesResponse;

// Namespace operations
mod create_namespace;
mod delete_namespace;
mod get_namespace;
mod list_namespaces;
mod namespace_exists;

pub use create_namespace::CreateNamespaceResponse;
pub use delete_namespace::DeleteNamespaceResponse;
pub use get_namespace::GetNamespaceResponse;
pub use list_namespaces::ListNamespacesResponse;
pub use namespace_exists::NamespaceExistsResponse;

// Table operations
mod commit_multi_table_transaction;
mod commit_table;
mod create_table;
mod delete_table;
mod list_tables;
mod load_table;
mod register_table;
mod rename_table;
mod table_exists;

pub use commit_multi_table_transaction::CommitMultiTableTransactionResponse;
pub use commit_table::CommitTableResponse;
pub use create_table::CreateTableResponse;
pub use delete_table::DeleteTableResponse;
pub use list_tables::ListTablesResponse;
pub use load_table::LoadTableResponse;
pub use register_table::RegisterTableResponse;
pub use rename_table::RenameTableResponse;
pub use table_exists::TableExistsResponse;

// Configuration & Metrics
mod get_config;
mod table_metrics;

pub use get_config::GetConfigResponse;
pub use table_metrics::TableMetricsResponse;
