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

//! Metadata location tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-integration_test.go`:
//! - Verify metadata location is set on create
//! - Verify metadata location format
//! - Verify location changes on commit

use super::common::*;
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;

/// Test metadata location is set on table creation.
/// Corresponds to MinIO server test: "TestTablesIntegrationMetadataLocation"
#[minio_macros::test(no_bucket)]
async fn metadata_location_set_on_create(ctx: TestContext) {
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
    assert!(
        result.metadata_location.is_some(),
        "Metadata location should be set on creation"
    );

    let metadata_location = result.metadata_location.unwrap();
    assert!(
        !metadata_location.is_empty(),
        "Metadata location should not be empty"
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
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test metadata location format follows expected pattern.
/// Corresponds to MinIO server test: metadata location format validation
#[minio_macros::test(no_bucket)]
async fn metadata_location_format(ctx: TestContext) {
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
    let metadata_location = result.metadata_location.unwrap();

    // Metadata location should contain the table path structure
    // Format varies by implementation, but typically includes:
    // - s3:// scheme prefix
    // - metadata path component
    // MinIO may use different formats than AWS S3 Tables
    assert!(
        metadata_location.starts_with("s3://") || metadata_location.starts_with("s3a://"),
        "Metadata location should have S3 scheme, got: {}",
        metadata_location
    );
    assert!(
        metadata_location.contains("metadata"),
        "Metadata location should contain 'metadata' path component, got: {}",
        metadata_location
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
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test metadata location is consistent when loading table.
/// Corresponds to MinIO server test: metadata consistency
#[minio_macros::test(no_bucket)]
async fn metadata_location_consistent_on_load(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema = create_test_schema();
    let create_resp: CreateTableResponse = tables
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

    let create_metadata = create_resp
        .table_result()
        .unwrap()
        .metadata_location
        .clone();

    // Load table and verify metadata location matches
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

    let load_metadata = load_resp.table_result().unwrap().metadata_location.clone();

    assert_eq!(
        create_metadata, load_metadata,
        "Metadata location should be consistent between create and load"
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
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test that each table has a unique metadata location.
/// Corresponds to MinIO server test: unique metadata per table
#[minio_macros::test(no_bucket)]
async fn metadata_location_unique_per_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1 = rand_table_name();
    let table2 = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema = create_test_schema();

    // Create first table
    let resp1: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table1.clone(),
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    let metadata1 = resp1
        .table_result()
        .unwrap()
        .metadata_location
        .clone()
        .unwrap();

    // Create second table
    let resp2: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table2.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();
    let metadata2 = resp2
        .table_result()
        .unwrap()
        .metadata_location
        .clone()
        .unwrap();

    assert_ne!(
        metadata1, metadata2,
        "Each table should have a unique metadata location"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table1.clone())
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table2.clone())
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
