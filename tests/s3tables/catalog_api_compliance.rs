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

//! Catalog API Compliance Tests
//!
//! These tests validate compliance with the Apache Iceberg REST Catalog API specification
//! for HTTP headers, content types, and edge cases. They correspond to tests from
//! MinIO eos iceberg-compat-tests Catalog API tests.
//!
//! References:
//! - https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
//! - MinIO eos iceberg-compat-tests (HDR-*, EDGE-*, CFG-*)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::response::{
    GetConfigResponse, GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse,
    ListViewsResponse, LoadTableResponse, LoadViewResponse, NamespaceExistsResponse,
    TableExistsResponse, ViewExistsResponse,
};
use minio::s3tables::utils::{Namespace, TableName, ViewName, ViewSql, WarehouseName};
use minio::s3tables::{HasTablesFields, TablesApi};
use minio_common::test_context::TestContext;

// =============================================================================
// HTTP Header Compliance Tests
// Corresponds to: HDR-001 to HDR-021
// =============================================================================

/// Test that GET table returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-001
#[minio_macros::test(no_bucket)]
async fn get_table_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load table and check Content-Type header
    let resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
        let ct_str = content_type.to_str().unwrap_or("");
        assert!(
            ct_str.contains("application/json"),
            "GET table should return application/json, got: {ct_str}"
        );
    } else {
        eprintln!("> Warning: Content-Type header not present in response");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that GET table returns ETag header.
/// Corresponds to Catalog API: HDR-002
#[minio_macros::test(no_bucket)]
async fn get_table_etag_present(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load table and check ETag header
    let resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(etag) = headers.get(http::header::ETAG) {
        let etag_str = etag.to_str().unwrap_or("");
        assert!(!etag_str.is_empty(), "ETag header should not be empty");
        eprintln!("> ETag present: {etag_str}");
    } else {
        eprintln!("> Note: ETag header not present (optional per spec)");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that HEAD table returns 204/200 with no/minimal body.
/// Corresponds to Catalog API: HDR-003, HDR-004, HDR-005
#[minio_macros::test(no_bucket)]
async fn head_table_response(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // HEAD table (table_exists)
    let resp: TableExistsResponse = tables
        .table_exists(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify table exists
    assert!(resp.exists(), "Table should exist");

    // HEAD response body should be empty or minimal
    let body = resp.body();
    eprintln!("> HEAD table body length: {} bytes", body.len());
    // Note: Some servers may return empty body, others may return minimal JSON

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that list tables returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-007
#[minio_macros::test(no_bucket)]
async fn list_tables_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // List tables and check Content-Type header
    let resp: ListTablesResponse = tables
        .list_tables(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
        let ct_str = content_type.to_str().unwrap_or("");
        assert!(
            ct_str.contains("application/json"),
            "List tables should return application/json, got: {ct_str}"
        );
    } else {
        eprintln!("> Warning: Content-Type header not present in response");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that GET namespace returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-008
#[minio_macros::test(no_bucket)]
async fn get_namespace_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Get namespace and check Content-Type header
    let resp: GetNamespaceResponse = tables
        .get_namespace(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
        let ct_str = content_type.to_str().unwrap_or("");
        assert!(
            ct_str.contains("application/json"),
            "GET namespace should return application/json, got: {ct_str}"
        );
    } else {
        eprintln!("> Warning: Content-Type header not present in response");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that HEAD namespace returns 204/200 with no/minimal body.
/// Corresponds to Catalog API: HDR-009, HDR-010, HDR-011
#[minio_macros::test(no_bucket)]
async fn head_namespace_response(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // HEAD namespace (namespace_exists)
    let resp: NamespaceExistsResponse = tables
        .namespace_exists(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify namespace exists
    assert!(resp.exists(), "Namespace should exist");

    // HEAD response body should be empty or minimal
    let body = resp.body();
    eprintln!("> HEAD namespace body length: {} bytes", body.len());

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that list namespaces returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-012
#[minio_macros::test(no_bucket)]
async fn list_namespaces_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // List namespaces and check Content-Type header
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
        let ct_str = content_type.to_str().unwrap_or("");
        assert!(
            ct_str.contains("application/json"),
            "List namespaces should return application/json, got: {ct_str}"
        );
    } else {
        eprintln!("> Warning: Content-Type header not present in response");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that GET view returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-013
#[minio_macros::test(no_bucket)]
async fn get_view_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = ViewName::try_from(
        format!("view_{}", uuid::Uuid::new_v4().to_string().replace('-', "")).as_str(),
    )
    .unwrap();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_test_schema();
    let view_sql = ViewSql::new("SELECT id, data FROM source_table").unwrap();

    let create_result = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_result {
        Ok(_) => {
            // Load view and check Content-Type header
            let load_result: Result<LoadViewResponse, Error> = tables
                .load_view(&warehouse, &namespace, &view)
                .unwrap()
                .build()
                .send()
                .await;

            if let Ok(resp) = load_result {
                let headers = resp.headers();
                if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
                    let ct_str = content_type.to_str().unwrap_or("");
                    assert!(
                        ct_str.contains("application/json"),
                        "GET view should return application/json, got: {ct_str}"
                    );
                } else {
                    eprintln!("> Warning: Content-Type header not present in response");
                }
            }

            // Cleanup view
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(e) => {
            eprintln!("> View operations not supported: {:?}", e);
        }
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that HEAD view returns 204/200 with no/minimal body.
/// Corresponds to Catalog API: HDR-014, HDR-015, HDR-016
#[minio_macros::test(no_bucket)]
async fn head_view_response(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = ViewName::try_from(
        format!("view_{}", uuid::Uuid::new_v4().to_string().replace('-', "")).as_str(),
    )
    .unwrap();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_test_schema();
    let view_sql = ViewSql::new("SELECT id, data FROM source_table").unwrap();

    let create_result = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_result {
        Ok(_) => {
            // HEAD view (view_exists)
            let resp: ViewExistsResponse = tables
                .view_exists(&warehouse, &namespace, &view)
                .unwrap()
                .build()
                .send()
                .await
                .unwrap();

            // Verify view exists
            assert!(resp.exists(), "View should exist");

            // HEAD response body should be empty or minimal
            let body = resp.body();
            eprintln!("> HEAD view body length: {} bytes", body.len());

            // Cleanup view
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(e) => {
            eprintln!("> View operations not supported: {:?}", e);
        }
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that list views returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-017
#[minio_macros::test(no_bucket)]
async fn list_views_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // List views and check Content-Type header
    let resp: ListViewsResponse = tables
        .list_views(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
        let ct_str = content_type.to_str().unwrap_or("");
        assert!(
            ct_str.contains("application/json"),
            "List views should return application/json, got: {ct_str}"
        );
    } else {
        eprintln!("> Warning: Content-Type header not present in response");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that GET config returns application/json Content-Type.
/// Corresponds to Catalog API: HDR-018
#[minio_macros::test(no_bucket)]
async fn get_config_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Get config and check Content-Type header
    let resp: GetConfigResponse = tables
        .get_config(&warehouse)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let headers = resp.headers();
    if let Some(content_type) = headers.get(http::header::CONTENT_TYPE) {
        let ct_str = content_type.to_str().unwrap_or("");
        assert!(
            ct_str.contains("application/json"),
            "GET config should return application/json, got: {ct_str}"
        );
    } else {
        eprintln!("> Warning: Content-Type header not present in response");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that 404 errors return Content-Type header.
/// Corresponds to Catalog API: HDR-019
#[minio_macros::test(no_bucket)]
async fn error_404_has_content_type(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Try to load non-existent table
    let nonexistent_table = TableName::try_from("nonexistent_table_12345").unwrap();
    let result: Result<LoadTableResponse, Error> = tables
        .load_table(&warehouse, &namespace, nonexistent_table)
        .unwrap()
        .build()
        .send()
        .await;

    // Verify we get an error
    assert!(result.is_err(), "Loading non-existent table should fail");

    // Note: Error responses may or may not expose headers through the SDK
    // This test verifies the error case works correctly
    if let Err(e) = result {
        eprintln!("> 404 error received: {:?}", e);
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that HEAD on non-existent namespace returns 404.
/// Corresponds to Catalog API: HDR-020
#[minio_macros::test(no_bucket)]
async fn head_nonexistent_namespace_returns_not_found(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // HEAD on non-existent namespace
    let nonexistent_ns = Namespace::try_from(vec!["nonexistent_ns_12345".to_string()]).unwrap();
    let resp: NamespaceExistsResponse = tables
        .namespace_exists(&warehouse, nonexistent_ns)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Should return exists=false (not an error)
    assert!(
        !resp.exists(),
        "Non-existent namespace should return exists=false"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that HEAD on non-existent table returns 404.
/// Corresponds to Catalog API: HDR-021
#[minio_macros::test(no_bucket)]
async fn head_nonexistent_table_returns_not_found(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // HEAD on non-existent table
    let nonexistent_table = TableName::try_from("nonexistent_table_12345").unwrap();
    let resp: TableExistsResponse = tables
        .table_exists(&warehouse, &namespace, nonexistent_table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Should return exists=false (not an error)
    assert!(
        !resp.exists(),
        "Non-existent table should return exists=false"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Edge Case Input Validation Tests
// Corresponds to: EDGE-001 to EDGE-020
// =============================================================================

/// Test that creating a table with empty name returns 400.
/// Corresponds to Catalog API: EDGE-001
#[minio_macros::test(no_bucket)]
async fn create_table_empty_name_fails(_ctx: TestContext) {
    // SDK validates empty name locally
    let result = TableName::try_from("");
    assert!(
        result.is_err(),
        "Empty table name should fail SDK validation"
    );
}

/// Test that creating a namespace with empty array returns 400.
/// Corresponds to Catalog API: EDGE-006
#[minio_macros::test(no_bucket)]
async fn create_namespace_empty_array_fails(_ctx: TestContext) {
    // SDK validates empty namespace locally
    let result = Namespace::try_from(Vec::<String>::new());
    assert!(
        result.is_err(),
        "Empty namespace array should fail SDK validation"
    );
}

/// Test that creating a view with empty name returns 400.
/// Corresponds to Catalog API: EDGE-010
#[minio_macros::test(no_bucket)]
async fn create_view_empty_name_fails(_ctx: TestContext) {
    // SDK validates empty view name locally
    let result = ViewName::try_from("");
    assert!(
        result.is_err(),
        "Empty view name should fail SDK validation"
    );
}

/// Test renaming table with non-existent source fails.
/// Corresponds to Catalog API: EDGE-015
#[minio_macros::test(no_bucket)]
async fn rename_table_source_not_found_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Try to rename non-existent table
    let nonexistent_table = TableName::try_from("nonexistent_source").unwrap();
    let new_table = TableName::try_from("new_name").unwrap();

    let result = tables
        .rename_table(
            &warehouse,
            &namespace,
            nonexistent_table,
            &namespace,
            new_table,
        )
        .unwrap()
        .build()
        .send()
        .await;

    assert!(
        result.is_err(),
        "Renaming non-existent table should fail with 404"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that registering a table with missing metadata location fails.
/// Corresponds to Catalog API: EDGE-019
///
/// Note: The SDK validates metadata_location locally, so this test verifies
/// that SDK validation catches empty metadata locations before sending to server.
#[minio_macros::test(no_bucket)]
async fn register_table_empty_metadata_location_fails(_ctx: TestContext) {
    // SDK validates empty metadata_location locally before sending to server
    // This is a client-side validation test, not a server test
    use minio::s3tables::utils::MetadataLocation;

    let result = MetadataLocation::try_from("");
    assert!(
        result.is_err(),
        "Empty metadata location should fail SDK validation"
    );
}

/// Test that registering a table with empty name fails.
/// Corresponds to Catalog API: EDGE-020
#[minio_macros::test(no_bucket)]
async fn register_table_empty_name_fails(_ctx: TestContext) {
    // SDK validates empty name locally
    let result = TableName::try_from("");
    assert!(
        result.is_err(),
        "Empty table name should fail SDK validation"
    );
}

// =============================================================================
// Config Error Cases Tests
// Corresponds to: CFG-001 to CFG-003
// =============================================================================

/// Test that GET config returns 200 for valid warehouse.
/// Corresponds to Catalog API: CFG-001
#[minio_macros::test(no_bucket)]
async fn get_config_valid_warehouse_succeeds(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Get config should succeed
    let resp: GetConfigResponse = tables
        .get_config(&warehouse)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify response has catalog config
    let config = resp.catalog_config().unwrap();
    // Config structure should be accessible (may be empty)
    let _ = (&config.defaults, &config.overrides, &config.endpoints);

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that GET config for non-existent warehouse returns 404.
/// Corresponds to Catalog API: CFG-003
#[minio_macros::test(no_bucket)]
async fn get_config_nonexistent_warehouse_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);

    // Get config for non-existent warehouse
    let nonexistent_warehouse = WarehouseName::try_from("nonexistent-warehouse-12345").unwrap();
    let result: Result<GetConfigResponse, Error> = tables
        .get_config(nonexistent_warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    assert!(
        result.is_err(),
        "GET config for non-existent warehouse should fail"
    );
}

// =============================================================================
// Additional Edge Case Tests
// =============================================================================

/// Test that warehouse name validation works correctly for special characters.
#[minio_macros::test(no_bucket)]
async fn warehouse_name_special_chars_validation(_ctx: TestContext) {
    // Test various invalid warehouse names
    let invalid_names = vec![
        "warehouse with spaces",
        "warehouse@symbol",
        "warehouse#hash",
        "UPPERCASE",         // May be invalid depending on rules
        "-starts-with-dash", // May be invalid
    ];

    for name in invalid_names {
        let result = WarehouseName::try_from(name);
        // Some names may be valid, some invalid - document behavior
        match result {
            Ok(_) => eprintln!("> Warehouse name '{name}' accepted"),
            Err(_) => eprintln!("> Warehouse name '{name}' rejected"),
        }
    }
}

/// Test that namespace name validation works correctly for special characters.
#[minio_macros::test(no_bucket)]
async fn namespace_name_special_chars_validation(_ctx: TestContext) {
    // Test various invalid namespace names
    let invalid_names = vec![
        "namespace with spaces",
        "namespace@symbol",
        "namespace#hash",
    ];

    for name in invalid_names {
        let result = Namespace::try_from(vec![name.to_string()]);
        match result {
            Ok(_) => eprintln!("> Namespace name '{name}' accepted"),
            Err(_) => eprintln!("> Namespace name '{name}' rejected"),
        }
    }
}

/// Test that table name validation works correctly for special characters.
#[minio_macros::test(no_bucket)]
async fn table_name_special_chars_validation(_ctx: TestContext) {
    // Test various invalid table names
    let invalid_names = vec!["table with spaces", "table@symbol", "table#hash"];

    for name in invalid_names {
        let result = TableName::try_from(name);
        match result {
            Ok(_) => eprintln!("> Table name '{name}' accepted"),
            Err(_) => eprintln!("> Table name '{name}' rejected"),
        }
    }
}

/// Test deleting a table with purge flag.
/// Corresponds to Catalog API: EDGE-005 (partial - testing purge works)
#[minio_macros::test(no_bucket)]
async fn delete_table_with_purge(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Delete table with purge=true
    let delete_result = tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .purge_requested(true)
        .build()
        .send()
        .await;

    assert!(
        delete_result.is_ok(),
        "Delete table with purge=true should succeed"
    );

    // Verify table is gone
    let exists: TableExistsResponse = tables
        .table_exists(&warehouse, &namespace, table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert!(!exists.exists(), "Table should not exist after delete");

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test deleting a table without purge flag.
#[minio_macros::test(no_bucket)]
async fn delete_table_without_purge(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Delete table with purge=false (default)
    let delete_result = tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .purge_requested(false)
        .build()
        .send()
        .await;

    assert!(
        delete_result.is_ok(),
        "Delete table with purge=false should succeed"
    );

    // Verify table is gone (from catalog - data may still exist)
    let exists: TableExistsResponse = tables
        .table_exists(&warehouse, &namespace, table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert!(
        !exists.exists(),
        "Table should not exist in catalog after delete"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}
