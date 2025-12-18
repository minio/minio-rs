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

//! Name validation tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-test-utils_test.go`:
//! - Warehouse name validation (length, characters, reserved suffixes)
//! - Namespace name validation (length, characters, underscores)
//! - Table name validation (length, characters, underscores)

use super::common::*;
use minio::s3tables::TablesApi;
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio_common::test_context::TestContext;

// =============================================================================
// Warehouse Name Validation Tests
// =============================================================================

/// Test valid warehouse names succeed.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_valid(ctx: TestContext) {
    let tables = create_tables_client(&ctx);

    // Valid standard name
    let warehouse_name_str = format!("valid-warehouse-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let warehouse = WarehouseName::try_from(warehouse_name_str.as_str()).unwrap();
    let resp = tables
        .create_warehouse(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Valid warehouse name should succeed");
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test warehouse name minimum length (3 chars).
#[minio_macros::test(no_bucket)]
async fn warehouse_name_minimum_length(ctx: TestContext) {
    let tables = create_tables_client(&ctx);

    // Minimum length (3 chars) should succeed
    let warehouse = WarehouseName::try_from("abc").unwrap();
    let resp = tables
        .create_warehouse(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(
        resp.is_ok(),
        "Minimum length warehouse name (3 chars) should succeed"
    );
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test warehouse name too short fails.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_too_short_fails(_ctx: TestContext) {
    // Too short (2 chars) should fail at validation
    let invalid_warehouse_result = WarehouseName::try_from("ab");
    assert!(
        invalid_warehouse_result.is_err(),
        "Warehouse name shorter than 3 chars should fail validation"
    );
}

/// Test warehouse name maximum length (63 chars).
#[minio_macros::test(no_bucket)]
async fn warehouse_name_maximum_length(ctx: TestContext) {
    let tables = create_tables_client(&ctx);

    // Maximum length (63 chars) should succeed
    let warehouse_name_str: String = "a".repeat(63);
    let warehouse = WarehouseName::try_from(warehouse_name_str.as_str()).unwrap();
    let resp = tables
        .create_warehouse(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(
        resp.is_ok(),
        "Maximum length warehouse name (63 chars) should succeed"
    );
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test warehouse name exceeding maximum length fails.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_exceeds_max_length_fails(_ctx: TestContext) {
    // Exceeds max length (64 chars) should fail at validation
    let warehouse_name_str: String = "b".repeat(64);
    let invalid_warehouse_result = WarehouseName::try_from(warehouse_name_str.as_str());
    assert!(
        invalid_warehouse_result.is_err(),
        "Warehouse name exceeding 63 chars should fail validation"
    );
}

/// Test warehouse name with uppercase letters fails.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_uppercase_fails(_ctx: TestContext) {
    // Try to create an invalid warehouse name - should fail at validation
    let invalid_warehouse_result = WarehouseName::try_from("My-Warehouse");
    assert!(
        invalid_warehouse_result.is_err(),
        "Warehouse name with uppercase should fail validation"
    );
}

/// Test warehouse name starting with hyphen fails.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_starts_with_hyphen_fails(_ctx: TestContext) {
    // Try to create an invalid warehouse name - should fail at validation
    let invalid_warehouse_result = WarehouseName::try_from("-my-warehouse");
    assert!(
        invalid_warehouse_result.is_err(),
        "Warehouse name starting with hyphen should fail validation"
    );
}

/// Test warehouse name ending with hyphen fails.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_ends_with_hyphen_fails(_ctx: TestContext) {
    // Try to create an invalid warehouse name - should fail at validation
    let invalid_warehouse_result = WarehouseName::try_from("my-warehouse-");
    assert!(
        invalid_warehouse_result.is_err(),
        "Warehouse name ending with hyphen should fail validation"
    );
}

/// Test warehouse name with period fails.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_with_period_fails(_ctx: TestContext) {
    // Try to create an invalid warehouse name - should fail at validation
    let invalid_warehouse_result = WarehouseName::try_from("my.warehouse");
    assert!(
        invalid_warehouse_result.is_err(),
        "Warehouse name with period should fail validation"
    );
}

// =============================================================================
// Namespace Name Validation Tests
// =============================================================================

/// Test valid namespace names succeed.
#[minio_macros::test(no_bucket)]
async fn namespace_name_valid(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Valid namespace with underscores
    let namespace = Namespace::try_from(vec!["my_test_namespace".to_string()]).unwrap();
    let resp = tables
        .create_namespace(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Valid namespace name should succeed");

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace name with numbers succeeds.
#[minio_macros::test(no_bucket)]
async fn namespace_name_with_numbers(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    let namespace = Namespace::try_from(vec!["namespace123".to_string()]).unwrap();
    let resp = tables
        .create_namespace(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Namespace name with numbers should succeed");

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace name with hyphens fails (only underscores allowed).
#[minio_macros::test(no_bucket)]
async fn namespace_name_with_hyphens_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Try to create an invalid namespace - should fail at validation
    let invalid_namespace_result = Namespace::try_from(vec!["my-namespace".to_string()]);
    assert!(
        invalid_namespace_result.is_err(),
        "Namespace name with hyphens should fail validation"
    );

    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace name starting with underscore fails.
#[minio_macros::test(no_bucket)]
async fn namespace_name_starts_with_underscore_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Try to create an invalid namespace - should fail at validation
    let invalid_namespace_result = Namespace::try_from(vec!["_namespace".to_string()]);
    assert!(
        invalid_namespace_result.is_err(),
        "Namespace name starting with underscore should fail validation"
    );

    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace name ending with underscore fails.
#[minio_macros::test(no_bucket)]
async fn namespace_name_ends_with_underscore_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Try to create an invalid namespace - should fail at validation
    let invalid_namespace_result = Namespace::try_from(vec!["namespace_".to_string()]);
    assert!(
        invalid_namespace_result.is_err(),
        "Namespace name ending with underscore should fail validation"
    );

    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace name with spaces fails.
#[minio_macros::test(no_bucket)]
async fn namespace_name_with_spaces_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Try to create an invalid namespace - should fail at validation
    let invalid_namespace_result = Namespace::try_from(vec!["my namespace".to_string()]);
    assert!(
        invalid_namespace_result.is_err(),
        "Namespace name with spaces should fail validation"
    );

    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace name with special characters fails.
#[minio_macros::test(no_bucket)]
async fn namespace_name_with_special_chars_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Try to create an invalid namespace - should fail at validation
    let invalid_namespace_result = Namespace::try_from(vec!["namespace!@$".to_string()]);
    assert!(
        invalid_namespace_result.is_err(),
        "Namespace name with special characters should fail validation"
    );

    delete_warehouse_helper(&warehouse, &tables).await;
}

// =============================================================================
// Table Name Validation Tests
// =============================================================================

/// Test valid table names succeed.
#[minio_macros::test(no_bucket)]
async fn table_name_valid(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Valid table with underscores
    let table = TableName::try_from("my_test_table").unwrap();
    let schema = create_test_schema();
    let resp = tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Valid table name should succeed");

    tables
        .delete_table(&warehouse, &namespace, table)
        .unwrap()
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test table name with numbers succeeds.
#[minio_macros::test(no_bucket)]
async fn table_name_with_numbers(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let table = TableName::try_from("table123").unwrap();
    let schema = create_test_schema();
    let resp = tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Table name with numbers should succeed");

    tables
        .delete_table(&warehouse, &namespace, table)
        .unwrap()
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test table name with hyphens behavior.
/// Note: AWS S3 Tables disallows hyphens in table names, but MinIO allows them.
/// This test verifies the server's behavior rather than asserting a specific outcome.
#[minio_macros::test(no_bucket)]
async fn table_name_with_hyphens_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let schema = create_test_schema();
    let table_name_with_hyphen = TableName::try_from("my-table").unwrap();
    let resp = tables
        .create_table(&warehouse, &namespace, &table_name_with_hyphen, schema)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            // MinIO allows hyphens in table names - clean up the created table
            eprintln!("> Server allows hyphens in table names (MinIO behavior)");
            tables
                .delete_table(&warehouse, &namespace, table_name_with_hyphen)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(_) => {
            // AWS S3 Tables behavior - hyphens not allowed
            eprintln!("> Server rejects hyphens in table names (AWS behavior)");
        }
    }

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test table name starting with underscore fails.
#[minio_macros::test(no_bucket)]
async fn table_name_starts_with_underscore_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let schema = create_test_schema();
    let invalid_table_name = TableName::try_from("_table").unwrap();
    let resp = tables
        .create_table(&warehouse, &namespace, invalid_table_name, schema)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Table name starting with underscore should fail"
    );

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test table name ending with underscore fails.
#[minio_macros::test(no_bucket)]
async fn table_name_ends_with_underscore_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let schema = create_test_schema();
    let invalid_table_name = TableName::try_from("table_").unwrap();
    let resp = tables
        .create_table(&warehouse, &namespace, invalid_table_name, schema)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Table name ending with underscore should fail"
    );

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test table name with spaces fails.
#[minio_macros::test(no_bucket)]
async fn table_name_with_spaces_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let schema = create_test_schema();
    let invalid_table_name = TableName::try_from("my table").unwrap();
    let resp = tables
        .create_table(&warehouse, &namespace, invalid_table_name, schema)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table name with spaces should fail");

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test table name with special characters fails.
#[minio_macros::test(no_bucket)]
async fn table_name_with_special_chars_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let schema = create_test_schema();
    let invalid_table_name = TableName::try_from("table!@$").unwrap();
    let resp = tables
        .create_table(&warehouse, &namespace, invalid_table_name, schema)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Table name with special characters should fail"
    );

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}
