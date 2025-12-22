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

// Common helper functions for all tables tests
mod common;

// Test data generation module
mod iceberg_test_data_generator;

// Integration tests for test data generation
mod iceberg_test_data_creation;

// Full end-to-end DataFusion TableProvider integration tests
#[cfg(feature = "datafusion")]
mod datafusion_full_integration;

// Tier 2 (Advanced) module tests
mod advanced;

// Module declarations for integration tests
mod comprehensive;
mod concurrent_operations;
mod create_delete;
mod create_table_options;
mod drop_table;
mod error_handling;
mod get_config;
mod get_namespace;
mod get_warehouse;
mod list_namespaces;
mod list_tables;
mod list_warehouses;
mod load_table;
mod load_table_credentials;
mod metadata_location;
mod name_validation;
mod namespace_exists;
mod namespace_properties;
mod rck_inspired;
mod register_table;
mod rename_table;
mod scan_planning;
mod table_exists;
mod table_metrics;
mod table_properties;
mod update_namespace_properties;
mod view_operations;
