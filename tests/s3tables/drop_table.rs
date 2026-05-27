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

//! Drop table tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-integration_test.go`:
//! - Drop table without purge (catalog only)
//! - Drop table with purge (deletes data)
//! - Verify table is removed from catalog

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::response::LoadTableResponse;
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;

/// Test dropping a table without purge.
/// Corresponds to MinIO server test: "TestTablesIntegrationDropTable" - drop without purge
#[minio_macros::test(no_bucket)]
async fn drop_table_without_purge(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Verify table exists
    let resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.table_result().is_ok());

    // Drop table without purge (default)
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify table is removed from catalog
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after drop");

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test dropping a table with purge.
/// Corresponds to MinIO server test: "TestTablesIntegrationDropTable" - drop with purge
#[minio_macros::test(no_bucket)]
async fn drop_table_with_purge(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Verify table exists
    let resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.table_result().is_ok());

    // Drop table with purge
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .purge_requested(true)
        .build()
        .send()
        .await
        .unwrap();

    // Verify table is removed from catalog
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after purge");

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test dropping multiple tables in sequence.
/// Corresponds to MinIO server test: sequential table deletion
#[minio_macros::test(no_bucket)]
async fn drop_multiple_tables(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1 = rand_table_name();
    let table2 = rand_table_name();
    let table3 = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table1, &tables).await;
    create_table_helper(&warehouse, &namespace, &table2, &tables).await;
    create_table_helper(&warehouse, &namespace, &table3, &tables).await;

    // Drop all tables
    for table in [&table1, &table2, &table3] {
        tables
            .delete_table(&warehouse, &namespace, table)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();

        // Verify each is gone
        let resp: Result<LoadTableResponse, Error> = tables
            .load_table(&warehouse, &namespace, table)
            .unwrap()
            .build()
            .send()
            .await;
        assert!(
            resp.is_err(),
            "Table {} should not exist after drop",
            table.as_str()
        );
    }

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}
