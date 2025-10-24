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

// Module declarations for tables integration tests
mod test_tables_commit_table;
mod test_tables_create_delete;
mod test_tables_get_config;
mod test_tables_get_namespace;
mod test_tables_get_warehouse;
mod test_tables_list_namespaces;
mod test_tables_list_tables;
mod test_tables_list_warehouses;
mod test_tables_load_table;
mod test_tables_multi_table_transaction;
mod test_tables_namespace_properties;
mod test_tables_register_table;
mod test_tables_rename_table;
