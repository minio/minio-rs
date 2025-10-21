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

// Tier 2 (Advanced) module tests
mod advanced;

// Module declarations for Tier 1 (main) integration tests
mod commit_table;
mod comprehensive;
mod create_delete;
mod get_config;
mod get_namespace;
mod get_warehouse;
mod list_namespaces;
mod list_tables;
mod list_warehouses;
mod load_table;
mod multi_table_transaction;
mod namespace_exists;
mod namespace_properties;
mod register_table;
mod rename_table;
mod table_exists;
