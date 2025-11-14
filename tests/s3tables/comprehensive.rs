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

//! Comprehensive integration tests for all Tables API operations and trait functionality

use super::common::*;
use minio::s3tables::response::{
    CreateNamespaceResponse, CreateTableResponse, CreateWarehouseResponse, DeleteTableResponse,
    DeleteWarehouseResponse, GetWarehouseResponse, LoadTableResponse,
};
use minio::s3tables::response_traits::{
    HasNamespace, HasTableResult, HasTablesFields, HasWarehouseName,
};
use minio::s3tables::{HasNamespacesResponse, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

// ============================================================================
// WAREHOUSE TRAIT TESTS
// ============================================================================

#[minio_macros::test(no_bucket)]
async fn test_warehouse_trait_accessors(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    let resp: CreateWarehouseResponse = tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");
    assert_eq!(resp.warehouse_name().unwrap(), warehouse_name);
    assert!(
        !resp.headers().is_empty(),
        "Response headers should not be empty"
    );
    assert!(!resp.body().is_empty(), "Response body should not be empty");
    assert!(
        !resp.request().path.is_empty(),
        "Request path should not be empty"
    );

    // Cleanup - ignore errors as warehouse may not be empty
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
    // Note: DeleteWarehouse returns 204 No Content
}

#[minio_macros::test(no_bucket)]
async fn test_get_warehouse_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    let resp: CreateWarehouseResponse = tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");
    assert_eq!(resp.warehouse_name().unwrap(), warehouse_name);

    let resp: GetWarehouseResponse = tables
        .get_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to get warehouse");
    assert_eq!(resp.warehouse_name().unwrap(), warehouse_name);

    let _resp: DeleteWarehouseResponse = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to delete warehouse");
    // Note: DeleteWarehouse returns 204 No Content
}

// ============================================================================
// NAMESPACE TRAIT TESTS
// ============================================================================

#[minio_macros::test(no_bucket)]
async fn test_namespace_trait_accessors(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();

    // Setup warehouse
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    // Create namespace
    let resp: CreateNamespaceResponse = tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create namespace");

    assert_eq!(resp.namespace(), namespace_name);

    // Test that parsed_namespace() returns the parsed response data
    let parsed_ns: Vec<String> = resp.namespaces_from_result().unwrap();
    assert_eq!(parsed_ns, vec![namespace_name.clone()]);

    // Cleanup
    let _ = tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
}

#[minio_macros::test(no_bucket)]
async fn test_get_namespace_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();

    // Setup
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create namespace");

    // Get namespace and test trait
    let get_resp = tables
        .get_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to get namespace");

    assert_eq!(
        get_resp.namespace(),
        namespace_name,
        "GetNamespace response should implement HasNamespace trait"
    );

    // Cleanup
    let _ = tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
}

// ============================================================================
// TABLE TRAIT TESTS
// ============================================================================

// #[minio_macros::test(no_bucket)]
#[allow(dead_code)]
async fn test_table_trait_accessors(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let schema = create_test_schema();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    let resp: CreateTableResponse = tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema,
        )
        .build()
        .send()
        .await
        .expect("Failed to create table");

    // Test HasTablesFields trait
    assert!(
        !resp.headers().is_empty(),
        "Table response headers should not be empty"
    );
    assert!(
        !resp.body().is_empty(),
        "Table response body should not be empty"
    );

    // Cleanup
    let _ = tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    let _ = tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
}

#[minio_macros::test(no_bucket)]
async fn test_load_table_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let schema = create_test_schema();

    // Setup
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create namespace");

    tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema,
        )
        .build()
        .send()
        .await
        .expect("Failed to create table");

    // Load table and test trait
    let resp: LoadTableResponse = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .expect("Failed to load table");
    // Verify table_result trait works
    let _ = resp
        .table_result()
        .expect("Failed to get table result from LoadTable response");

    // Cleanup
    let _ = tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    let _ = tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
}

// ============================================================================
// COMPREHENSIVE API COVERAGE
// ============================================================================

// #[minio_macros::test(no_bucket)]
#[allow(dead_code)]
async fn test_warehouse_list_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse1 = rand_warehouse_name();
    let warehouse2 = rand_warehouse_name();

    // Create warehouses
    tables
        .create_warehouse(&warehouse1)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse1");

    tables
        .create_warehouse(&warehouse2)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse2");

    // List warehouses
    let resp = tables
        .list_warehouses()
        .build()
        .send()
        .await
        .expect("Failed to list warehouses");

    // Test HasTablesFields trait
    assert!(!resp.headers().is_empty());
    assert!(!resp.body().is_empty());

    let warehouses = resp.warehouses().expect("Failed to parse warehouses");
    assert!(warehouses.iter().any(|w| w == &warehouse1));
    assert!(warehouses.iter().any(|w| w == &warehouse2));

    // Cleanup
    let _ = tables.delete_warehouse(&warehouse1).build().send().await;
    let _ = tables.delete_warehouse(&warehouse2).build().send().await;
}

#[minio_macros::test(no_bucket)]
async fn test_namespace_list_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let ns1 = rand_namespace_name();
    let ns2 = rand_namespace_name();

    // Setup warehouse
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    // Create namespaces
    tables
        .create_namespace(&warehouse_name, vec![ns1.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create ns1");

    tables
        .create_namespace(&warehouse_name, vec![ns2.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create ns2");

    // List namespaces
    let list_resp = tables
        .list_namespaces(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to list namespaces");

    // Test HasTablesFields trait
    assert!(!list_resp.headers().is_empty());

    let namespaces = list_resp.namespaces().expect("Failed to parse namespaces");
    assert!(namespaces.iter().any(|ns| ns == &vec![ns1.clone()]));
    assert!(namespaces.iter().any(|ns| ns == &vec![ns2.clone()]));

    // Cleanup
    let _ = tables
        .delete_namespace(&warehouse_name, vec![ns1])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_namespace(&warehouse_name, vec![ns2])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
}

#[minio_macros::test(no_bucket)]
async fn test_table_list_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table1 = rand_table_name();
    let table2 = rand_table_name();
    let schema = create_test_schema();

    // Setup
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create namespace");

    // Create tables
    tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table1,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .expect("Failed to create table1");

    tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table2,
            schema,
        )
        .build()
        .send()
        .await
        .expect("Failed to create table2");

    // List tables
    let list_resp = tables
        .list_tables(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to list tables");

    // Test HasTablesFields trait
    assert!(!list_resp.headers().is_empty());

    let identifiers = list_resp
        .identifiers()
        .expect("Failed to parse table identifiers");
    let names: Vec<String> = identifiers.iter().map(|id| id.name.clone()).collect();
    assert!(names.contains(&table1));
    assert!(names.contains(&table2));

    // Cleanup
    let _ = tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table1)
        .build()
        .send()
        .await;
    let _ = tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table2)
        .build()
        .send()
        .await;
    let _ = tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await;
    let _ = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
}

#[minio_macros::test(no_bucket)]
async fn test_table_delete_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let schema = create_test_schema();

    // Setup
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .expect("Failed to create namespace");

    tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema,
        )
        .build()
        .send()
        .await
        .expect("Failed to create table");

    // Delete table and test trait
    let resp: DeleteTableResponse = tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .expect("Failed to delete table");
    // Note: DeleteTable returns 204 No Content, verify it's empty
    assert!(resp.body().is_empty());

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(&warehouse_name, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn test_get_config_tables_fields_trait(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(&warehouse_name, &tables).await;

    // Get config and test HasTablesFields trait
    let config_resp = tables
        .get_config(&warehouse_name)
        .build()
        .send()
        .await
        .expect("Failed to get config");

    assert!(
        !config_resp.headers().is_empty(),
        "GetConfig response headers should not be empty"
    );
    assert!(
        !config_resp.body().is_empty(),
        "GetConfig response body should not be empty"
    );
    assert!(
        !config_resp.request().path.is_empty(),
        "Request path should not be empty"
    );

    delete_warehouse_helper(&warehouse_name, &tables).await;
}
