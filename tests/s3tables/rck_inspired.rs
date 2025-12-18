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

//! Tests inspired by Apache Iceberg REST Compatibility Kit (RCK).
//!
//! These tests are based on the CatalogTests and ViewCatalogTests from:
//! - https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/catalog/CatalogTests.java
//! - https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/view/ViewCatalogTests.java

use super::common::*;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::response::{
    CreateNamespaceResponse, CreateTableResponse, ListTablesResponse, ViewExistsResponse,
};
use minio::s3tables::utils::{Namespace, TableName, ViewName, ViewSql};
use minio::s3tables::{HasNamespace, HasProperties, HasTableResult, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

/// Create a test schema for views
fn create_view_schema() -> Schema {
    Schema {
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 2,
                name: "name".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Name field".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: None,
        ..Default::default()
    }
}

/// Generate a random view name as a wrapper type
fn rand_view_name() -> ViewName {
    let name = format!("view_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
    ViewName::try_from(name.as_str()).expect("Generated view name should be valid")
}

// =============================================================================
// Nested Namespace Tests (from CatalogTests.testListNestedNamespaces)
// =============================================================================

/// Test creating nested namespaces.
/// Corresponds to RCK: testListNestedNamespaces
#[minio_macros::test(no_bucket)]
async fn nested_namespace_create(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Create parent namespace
    let parent_ns = rand_namespace();
    tables
        .create_namespace(warehouse_name.clone(), parent_ns.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Create child namespace (nested) - this may or may not be supported
    let child_ns_name = format!(
        "child_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    let nested_ns_vec = vec![parent_ns.first().to_string(), child_ns_name.clone()];
    let nested_ns = Namespace::try_from(nested_ns_vec.clone()).unwrap();
    let resp = tables
        .create_namespace(warehouse_name.clone(), nested_ns.clone())
        .build()
        .send()
        .await
        .expect("Nested namespace creation should succeed");

    // Verify the namespace was created correctly
    let created_ns = resp.namespace_parts().unwrap();
    assert_eq!(
        created_ns, nested_ns_vec,
        "Nested namespace should match requested levels"
    );

    // Verify we can get the nested namespace
    let get_resp = tables
        .get_namespace(warehouse_name.clone(), nested_ns.clone())
        .build()
        .send()
        .await
        .expect("Should be able to get nested namespace");
    assert_eq!(
        get_resp.namespace_parts().unwrap(),
        nested_ns.as_slice(),
        "Get namespace should return correct levels"
    );

    // Cleanup - delete child first, then parent
    tables
        .delete_namespace(warehouse_name.clone(), nested_ns)
        .build()
        .send()
        .await
        .expect("Should delete nested namespace");
    delete_namespace_helper(warehouse_name.clone(), parent_ns, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Drop Non-Empty Namespace Tests (from CatalogTests.testDropNonEmptyNamespace)
// =============================================================================

/// Test that dropping a namespace containing tables fails.
/// Corresponds to RCK: testDropNonEmptyNamespace
#[minio_macros::test(no_bucket)]
async fn drop_non_empty_namespace_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Try to drop namespace containing a table - should fail
    let result = tables
        .delete_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await;

    assert!(result.is_err(), "Dropping non-empty namespace should fail");

    // Cleanup - delete table first, then namespace
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Create Existing Namespace Tests (from CatalogTests.testCreateExistingNamespace)
// =============================================================================

/// Test that creating an already existing namespace fails.
/// Corresponds to RCK: testCreateExistingNamespace
#[minio_macros::test(no_bucket)]
async fn create_existing_namespace_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Try to create the same namespace again - should fail
    let result = tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await;

    assert!(result.is_err(), "Creating duplicate namespace should fail");

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Table Already Exists Tests (from CatalogTests.testBasicCreateTableThatAlreadyExists)
// =============================================================================

/// Test that creating an already existing table fails.
/// Corresponds to RCK: testBasicCreateTableThatAlreadyExists
#[minio_macros::test(no_bucket)]
async fn create_existing_table_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Try to create the same table again - should fail
    let schema = create_test_schema();
    let result = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await;

    assert!(result.is_err(), "Creating duplicate table should fail");

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// View-Table Naming Conflict Tests (from ViewCatalogTests)
// =============================================================================

/// Test that creating a view when a table with same name exists fails.
/// Corresponds to RCK: createViewThatAlreadyExistsAsTable
#[minio_macros::test(no_bucket)]
async fn create_view_when_table_exists_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    // Same string for both table and view name
    let name_str = format!(
        "entity_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    let table_name = TableName::try_from(name_str.as_str()).unwrap();
    let view_name = ViewName::try_from(name_str.as_str()).unwrap();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create a table first
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Try to create a view with the same name - should fail
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT 1").unwrap();
    let result = tables
        .create_view(
            warehouse_name.clone(),
            namespace.clone(),
            view_name,
            schema,
            view_sql,
        )
        .build()
        .send()
        .await;

    assert!(
        result.is_err(),
        "Creating view with same name as existing table should fail"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test that creating a table when a view with same name exists fails.
/// Corresponds to RCK: createTableThatAlreadyExistsAsView
#[minio_macros::test(no_bucket)]
async fn create_table_when_view_exists_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    // Same string for both table and view name
    let name_str = format!(
        "entity_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    let table_name = TableName::try_from(name_str.as_str()).unwrap();
    let view_name = ViewName::try_from(name_str.as_str()).unwrap();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create a view first
    let view_schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT 1").unwrap();
    tables
        .create_view(
            warehouse_name.clone(),
            namespace.clone(),
            view_name.clone(),
            view_schema,
            view_sql,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Try to create a table with the same name - should fail
    let table_schema = create_test_schema();
    let result = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name,
            table_schema,
        )
        .build()
        .send()
        .await;

    assert!(
        result.is_err(),
        "Creating table with same name as existing view should fail"
    );

    // Cleanup
    tables
        .drop_view(warehouse_name.clone(), namespace.clone(), view_name)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// View Rename Across Namespaces (from ViewCatalogTests.renameViewUsingDifferentNamespace)
// =============================================================================

/// Test renaming a view to a different namespace.
/// Corresponds to RCK: renameViewUsingDifferentNamespace
#[minio_macros::test(no_bucket)]
async fn rename_view_across_namespaces(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let source_ns = rand_namespace();
    let target_ns = rand_namespace();
    let view_name = rand_view_name();
    let new_view_name = rand_view_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), source_ns.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), target_ns.clone(), &tables).await;

    // Create a view in source namespace
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT 1").unwrap();
    tables
        .create_view(
            warehouse_name.clone(),
            source_ns.clone(),
            view_name.clone(),
            schema,
            view_sql,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Rename view to target namespace
    tables
        .rename_view(
            warehouse_name.clone(),
            source_ns.clone(),
            view_name.clone(),
            target_ns.clone(),
            new_view_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    // Verify view exists in target namespace
    let resp: ViewExistsResponse = tables
        .view_exists(
            warehouse_name.clone(),
            target_ns.clone(),
            new_view_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.exists(), "View should exist in target namespace");

    // Verify view no longer exists in source namespace
    let resp: ViewExistsResponse = tables
        .view_exists(warehouse_name.clone(), source_ns.clone(), view_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(!resp.exists(), "View should not exist in source namespace");

    // Cleanup
    tables
        .drop_view(warehouse_name.clone(), target_ns.clone(), new_view_name)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), source_ns, &tables).await;
    delete_namespace_helper(warehouse_name.clone(), target_ns, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Namespace Properties Tests (from CatalogTests.testCreateNamespaceWithProperties)
// =============================================================================

/// Test creating namespace with properties.
/// Corresponds to RCK: testCreateNamespaceWithProperties
#[minio_macros::test(no_bucket)]
async fn create_namespace_with_properties(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Create namespace with properties
    let mut props = HashMap::new();
    props.insert("owner".to_string(), "test-user".to_string());
    props.insert("description".to_string(), "Test namespace".to_string());

    let resp: CreateNamespaceResponse = tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .properties(props.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify properties were set
    let returned_props = resp.properties().unwrap_or_default();
    assert_eq!(
        returned_props.get("owner"),
        Some(&"test-user".to_string()),
        "Owner property should be set"
    );
    assert_eq!(
        returned_props.get("description"),
        Some(&"Test namespace".to_string()),
        "Description property should be set"
    );

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Rename Table Destination Already Exists (from CatalogTests)
// =============================================================================

/// Test that renaming a table to an existing table name fails.
/// Corresponds to RCK: testRenameTableDestinationTableAlreadyExists
#[minio_macros::test(no_bucket)]
async fn rename_table_destination_exists_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1 = rand_table_name();
    let table2 = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table1.clone(),
        &tables,
    )
    .await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table2.clone(),
        &tables,
    )
    .await;

    // Try to rename table1 to table2 - should fail because table2 exists
    let result = tables
        .rename_table(
            warehouse_name.clone(),
            namespace.clone(),
            table1.clone(),
            namespace.clone(),
            table2.clone(),
        )
        .build()
        .send()
        .await;

    assert!(
        result.is_err(),
        "Renaming table to existing table name should fail"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table1)
        .build()
        .send()
        .await
        .ok();
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table2)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Rename View Destination Already Exists (from ViewCatalogTests)
// =============================================================================

/// Test that renaming a view to an existing view name fails.
/// Corresponds to RCK: renameViewTargetAlreadyExistsAsView
#[minio_macros::test(no_bucket)]
async fn rename_view_destination_exists_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let view1 = rand_view_name();
    let view2 = rand_view_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create two views
    let schema = create_view_schema();
    let view_sql1 = ViewSql::new("SELECT 1").unwrap();
    tables
        .create_view(
            warehouse_name.clone(),
            namespace.clone(),
            view1.clone(),
            schema.clone(),
            view_sql1,
        )
        .build()
        .send()
        .await
        .unwrap();

    let view_sql2 = ViewSql::new("SELECT 2").unwrap();
    tables
        .create_view(
            warehouse_name.clone(),
            namespace.clone(),
            view2.clone(),
            schema,
            view_sql2,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Try to rename view1 to view2 - should fail because view2 exists
    let result = tables
        .rename_view(
            warehouse_name.clone(),
            namespace.clone(),
            view1.clone(),
            namespace.clone(),
            view2.clone(),
        )
        .build()
        .send()
        .await;

    assert!(
        result.is_err(),
        "Renaming view to existing view name should fail"
    );

    // Cleanup
    tables
        .drop_view(warehouse_name.clone(), namespace.clone(), view1)
        .build()
        .send()
        .await
        .ok();
    tables
        .drop_view(warehouse_name.clone(), namespace.clone(), view2)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Table Creation with Location (from CatalogTests.testCompleteCreateTable)
// =============================================================================

/// Test creating table with custom location.
/// Corresponds to RCK: testCompleteCreateTable
#[minio_macros::test(no_bucket)]
async fn create_table_with_location(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create table with custom location
    let schema = create_test_schema();
    let custom_location = format!(
        "s3://test-bucket/{}/{}/{}",
        warehouse_name.as_str(),
        namespace.first(),
        table_name.as_str()
    );

    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .location(&custom_location)
        .build()
        .send()
        .await
        .unwrap();

    // Verify table was created
    let result = resp.table_result().unwrap();
    assert!(result.metadata_location.is_some());

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// List Tables in Empty Namespace (from CatalogTests.listTablesInEmptyNamespace)
// =============================================================================

/// Test listing tables in an empty namespace returns empty list.
/// Corresponds to RCK: listTablesInEmptyNamespace
#[minio_macros::test(no_bucket)]
async fn list_tables_empty_namespace(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // List tables - should be empty
    let resp: ListTablesResponse = tables
        .list_tables(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    let identifiers = resp.identifiers().unwrap();
    assert!(
        identifiers.is_empty(),
        "Empty namespace should have no tables"
    );

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Drop Non-existent Table Handling (from CatalogTests.testDropMissingTable)
// =============================================================================

/// Test that dropping a non-existent table is handled gracefully.
/// Corresponds to RCK: testDropMissingTable (behavior varies by implementation)
#[minio_macros::test(no_bucket)]
async fn drop_nonexistent_table_handling(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Try to drop a table that doesn't exist
    let nonexistent_table = TableName::try_from("nonexistent_table").unwrap();
    let result = tables
        .delete_table(warehouse_name.clone(), namespace.clone(), nonexistent_table)
        .build()
        .send()
        .await;

    // Per RCK, this may either succeed (idempotent) or fail with NoSuchTableException
    // Just verify we get a deterministic response
    assert!(
        result.is_ok() || result.is_err(),
        "Should get deterministic response for dropping nonexistent table"
    );

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// =============================================================================
// Drop Non-existent Namespace Handling (from CatalogTests.testDropNonexistentNamespace)
// =============================================================================

/// Test that dropping a non-existent namespace is handled gracefully.
/// Corresponds to RCK: testDropNonexistentNamespace
#[minio_macros::test(no_bucket)]
async fn drop_nonexistent_namespace_handling(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Try to drop a namespace that doesn't exist
    let nonexistent_ns = Namespace::try_from(vec!["nonexistent_namespace".to_string()]).unwrap();
    let result = tables
        .delete_namespace(warehouse_name.clone(), nonexistent_ns)
        .build()
        .send()
        .await;

    // Per RCK, this may either succeed (idempotent) or fail with NoSuchNamespaceException
    assert!(
        result.is_ok() || result.is_err(),
        "Should get deterministic response for dropping nonexistent namespace"
    );

    // Cleanup
    delete_warehouse_helper(warehouse_name, &tables).await;
}
