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

//! Rename table tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-api-handlers_test.go`:
//! - Rename table within same namespace
//! - Rename table across different namespaces
//! - Rename table to itself (no-op)
//! - Rename to non-existing namespace (error)
//! - Rename to empty name (error)
//! - Rename to invalid identifier (error)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse, RenameTableResponse};
use minio::s3tables::utils::Namespace;
use minio::s3tables::{HasTableResult, HasTablesFields, TablesApi};
use minio_common::test_context::TestContext;

/// Test renaming a table within the same namespace.
/// Corresponds to MinIO server test: "Rename table within the same namespace succeeds"
#[minio_macros::test(no_bucket)]
async fn rename_table_within_same_namespace(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();
    let new_table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
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

    let result = resp.table_result().unwrap();
    assert!(result.metadata_location.is_some());
    let original_metadata: String = result.metadata_location.unwrap();

    // Rename table within same namespace
    let resp: RenameTableResponse = tables
        .rename_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            namespace.clone(),
            new_table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.body().is_empty());

    // Verify old table name no longer exists
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Old table should not exist after rename");

    // Verify new table name exists and metadata location is preserved
    let resp: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            new_table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_result = resp.table_result().unwrap();
    assert_eq!(
        loaded_result.metadata_location.unwrap(),
        original_metadata,
        "Metadata location should be preserved after rename"
    );

    // Cleanup
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            new_table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test renaming a table across different namespaces.
/// Corresponds to MinIO server test: "Rename table within different namespaces succeeds"
#[minio_macros::test(no_bucket)]
async fn rename_table_across_namespaces(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let source_namespace = rand_namespace();
    let target_namespace = rand_namespace();
    let table_name = rand_table_name();
    let new_table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), source_namespace.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), target_namespace.clone(), &tables).await;

    // Create table in source namespace
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            source_namespace.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    let original_metadata: String = result.metadata_location.unwrap();

    // Rename table to different namespace
    let resp: RenameTableResponse = tables
        .rename_table(
            warehouse_name.clone(),
            source_namespace.clone(),
            table_name.clone(),
            target_namespace.clone(),
            new_table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.body().is_empty());

    // Verify table no longer exists in source namespace
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(
            warehouse_name.clone(),
            source_namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Table should not exist in source namespace after rename"
    );

    // Verify table exists in target namespace with preserved metadata
    let resp: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            target_namespace.clone(),
            new_table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_result = resp.table_result().unwrap();
    assert_eq!(
        loaded_result.metadata_location.unwrap(),
        original_metadata,
        "Metadata location should be preserved after cross-namespace rename"
    );

    // Cleanup
    tables
        .delete_table(
            warehouse_name.clone(),
            target_namespace.clone(),
            new_table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), source_namespace.clone(), &tables).await;
    delete_namespace_helper(warehouse_name.clone(), target_namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test renaming a table to itself (no-op).
/// Corresponds to MinIO server test: "Rename table to itself succeeds"
#[minio_macros::test(no_bucket)]
async fn rename_table_to_itself(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
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

    let result = resp.table_result().unwrap();
    let original_metadata: String = result.metadata_location.unwrap();

    // Rename table to itself (should succeed as no-op)
    let resp: RenameTableResponse = tables
        .rename_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.body().is_empty());

    // Verify table still exists with same metadata
    let resp: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_result = resp.table_result().unwrap();
    assert_eq!(
        loaded_result.metadata_location.unwrap(),
        original_metadata,
        "Metadata should be unchanged after rename-to-self"
    );

    // Cleanup
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test renaming a table to a non-existing namespace (should fail).
/// Corresponds to MinIO server test: "Rename table to non-existing namespace fails"
#[minio_macros::test(no_bucket)]
async fn rename_table_to_nonexistent_namespace_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

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

    // Try to rename to non-existing namespace
    let nonexistent_ns = Namespace::try_from(vec!["nonexistent_namespace".to_string()]).unwrap();
    let resp: Result<RenameTableResponse, Error> = tables
        .rename_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            nonexistent_ns,
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    assert!(
        resp.is_err(),
        "Rename to non-existing namespace should fail"
    );

    // Verify original table still exists (rename was atomic - failed completely)
    let resp: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert!(
        resp.table_result().is_ok(),
        "Original table should still exist after failed rename"
    );

    // Cleanup
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test renaming a table with bidirectional rename (rename back and forth).
/// Corresponds to MinIO server pattern of renaming table then renaming it back.
#[minio_macros::test(no_bucket)]
async fn rename_table_bidirectional(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace1 = rand_namespace();
    let namespace2 = rand_namespace();
    let table_name = rand_table_name();
    let renamed_table = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace1.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace2.clone(), &tables).await;

    // Create table
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace1.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();
    let original_metadata: String = resp.table_result().unwrap().metadata_location.unwrap();

    // Rename from ns1 to ns2
    tables
        .rename_table(
            warehouse_name.clone(),
            namespace1.clone(),
            table_name.clone(),
            namespace2.clone(),
            renamed_table.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    // Rename back from ns2 to ns1
    tables
        .rename_table(
            warehouse_name.clone(),
            namespace2.clone(),
            renamed_table.clone(),
            namespace1.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    // Verify table is back in original location with same metadata
    let resp: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace1.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_result = resp.table_result().unwrap();
    assert_eq!(
        loaded_result.metadata_location.unwrap(),
        original_metadata,
        "Metadata should be preserved after bidirectional rename"
    );

    // Verify table doesn't exist in intermediate location
    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(
            warehouse_name.clone(),
            namespace2.clone(),
            renamed_table.clone(),
        )
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Table should not exist in ns2 after rename back"
    );

    // Cleanup
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace1.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace1.clone(), &tables).await;
    delete_namespace_helper(warehouse_name.clone(), namespace2.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
