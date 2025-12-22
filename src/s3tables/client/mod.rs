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

//! S3 Tables client module

// Core client with pluggable authentication
mod tables_client;
pub use tables_client::{DEFAULT_BASE_PATH, TablesClient, TablesClientBuilder, base_paths};

// Warehouse operations
mod create_warehouse;
mod delete_warehouse;
mod get_warehouse;
mod list_warehouses;

// Namespace operations
mod create_namespace;
mod delete_namespace;
mod get_namespace;
mod list_namespaces;
mod namespace_exists;
mod update_namespace_properties;

// Table operations
mod commit_multi_table_transaction;
mod commit_table;
mod create_table;
mod delete_table;
mod list_tables;
mod load_table;
mod load_table_credentials;
mod register_table;
mod rename_table;
mod table_exists;

// View operations
mod create_view;
mod drop_view;
mod list_views;
mod load_view;
mod rename_view;
mod replace_view;
mod view_exists;

// Configuration & Metrics
mod get_config;
mod table_metrics;

// Scan planning operations
mod cancel_planning;
mod execute_table_scan;
mod fetch_planning_result;
mod fetch_scan_tasks;
mod plan_table_scan;
