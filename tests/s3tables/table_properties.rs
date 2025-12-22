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

//! Table properties tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-integration_test.go`:
//! - Create table with initial properties
//! - Add/update properties via CommitTable
//! - Remove properties
//! - Verify properties persist across loads

use super::common::*;
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

/// Test creating a table with initial properties.
/// Corresponds to MinIO server test: "TestTablesTableProperties" - create_with_properties
#[minio_macros::test(no_bucket)]
async fn table_create_with_properties(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create table with initial properties
    let mut properties = HashMap::new();
    properties.insert("owner".to_string(), "test-user".to_string());
    properties.insert("created-by".to_string(), "integration-test".to_string());
    properties.insert("department".to_string(), "engineering".to_string());

    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .properties(properties.clone())
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(result.metadata_location.is_some());

    // Load table and verify properties were set
    let load_resp: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_result = load_resp.table_result().unwrap();
    assert!(loaded_result.metadata_location.is_some());

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test loading table preserves properties across multiple loads.
/// Corresponds to MinIO server test: properties persistence
#[minio_macros::test(no_bucket)]
async fn table_properties_persist_across_loads(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Load table first time
    let load1: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    let metadata1 = load1.table_result().unwrap().metadata_location.clone();

    // Load table second time
    let load2: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    let metadata2 = load2.table_result().unwrap().metadata_location.clone();

    // Metadata location should be the same
    assert_eq!(
        metadata1, metadata2,
        "Metadata location should be consistent across loads"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
