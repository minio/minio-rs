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

//! Error handling tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-integration_test.go`:
//! - Not found errors (warehouse, namespace, table)
//! - Conflict errors (already exists)
//! - Load from non-existent resources

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response::LoadTableResponse;
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio_common::test_context::TestContext;

/// Test loading a table from a non-existent warehouse.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn load_table_from_nonexistent_warehouse_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);

    let warehouse = WarehouseName::try_from("nonexistent-warehouse").unwrap();
    let namespace = Namespace::try_from(vec!["nonexistent_ns".to_string()]).unwrap();
    let table = TableName::try_from("nonexistent_table").unwrap();

    // Try to load table from non-existent warehouse
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(warehouse, namespace, table)
        .build()
        .send()
        .await;

    // Expect some kind of error (warehouse or table not found)
    assert!(
        resp.is_err(),
        "Expected error loading from non-existent warehouse"
    );
}

/// Test loading a table from a non-existent namespace.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn load_table_from_nonexistent_namespace_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    let namespace = Namespace::try_from(vec!["nonexistent_ns".to_string()]).unwrap();
    let table = TableName::try_from("nonexistent_table").unwrap();

    // Try to load table from non-existent namespace
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(warehouse_name.clone(), namespace, table)
        .build()
        .send()
        .await;

    // Expect error (table or namespace not found)
    assert!(
        resp.is_err(),
        "Expected error loading from non-existent namespace"
    );

    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test loading a non-existent table.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn load_nonexistent_table_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let table = TableName::try_from("nonexistent_table").unwrap();

    // Try to load non-existent table
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(warehouse_name.clone(), namespace.clone(), table)
        .build()
        .send()
        .await;

    assert!(resp.is_err(), "Expected error loading non-existent table");

    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test getting a non-existent namespace.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn get_nonexistent_namespace_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    let namespace = Namespace::try_from(vec!["nonexistent_ns".to_string()]).unwrap();

    // Try to get non-existent namespace
    let resp: Result<_, Error> = tables
        .get_namespace(warehouse_name.clone(), namespace)
        .build()
        .send()
        .await;

    assert!(
        resp.is_err(),
        "Expected error getting non-existent namespace"
    );

    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test getting a non-existent warehouse.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn get_nonexistent_warehouse_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);

    let warehouse = WarehouseName::try_from("nonexistent-warehouse").unwrap();

    // Try to get non-existent warehouse
    let resp: Result<_, Error> = tables.get_warehouse(warehouse).build().send().await;

    assert!(
        resp.is_err(),
        "Expected error getting non-existent warehouse"
    );
}

/// Test deleting a non-existent table.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn delete_nonexistent_table_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let table = TableName::try_from("nonexistent_table").unwrap();

    // Try to delete non-existent table
    let resp: Result<_, Error> = tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table)
        .build()
        .send()
        .await;

    assert!(resp.is_err(), "Expected error deleting non-existent table");

    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test deleting a non-existent namespace.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn delete_nonexistent_namespace_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    let namespace = Namespace::try_from(vec!["nonexistent_ns".to_string()]).unwrap();

    // Try to delete non-existent namespace
    let resp: Result<_, Error> = tables
        .delete_namespace(warehouse_name.clone(), namespace)
        .build()
        .send()
        .await;

    assert!(
        resp.is_err(),
        "Expected error deleting non-existent namespace"
    );

    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test renaming a non-existent table.
/// Corresponds to MinIO server test: "TestTablesIntegrationErrorHandling" - not found errors
#[minio_macros::test(no_bucket)]
async fn rename_nonexistent_table_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let old_table = TableName::try_from("nonexistent_table").unwrap();
    let new_table = TableName::try_from("new_table_name").unwrap();

    // Try to rename non-existent table
    let resp: Result<_, Error> = tables
        .rename_table(
            warehouse_name.clone(),
            namespace.clone(),
            old_table,
            namespace.clone(),
            new_table,
        )
        .build()
        .send()
        .await;

    assert!(resp.is_err(), "Expected error renaming non-existent table");

    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
