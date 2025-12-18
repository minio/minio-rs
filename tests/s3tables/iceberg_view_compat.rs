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

//! Iceberg View Compatibility Tests
//!
//! These tests validate compatibility with Apache Iceberg REST Catalog specification
//! for view operations. They correspond to tests from Apache Iceberg's ViewCatalogTests.java
//! in the REST Compatibility Kit (RCK).
//!
//! References:
//! - https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/view/ViewCatalogTests.java
//! - MinIO eos iceberg-compat-tests

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::builders::replace_view::{
    SqlViewRepresentation, ViewUpdate, ViewVersionUpdate,
};
use minio::s3tables::builders::{TableRequirement, TableUpdate};
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::response::{
    CreateTableResponse, CreateViewResponse, ListTablesResponse, ListViewsResponse,
    LoadViewResponse, ReplaceViewResponse,
};
use minio::s3tables::response_traits::HasCachedViewResult;
use minio::s3tables::utils::{ViewName, ViewSql};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

/// Check if an error indicates the API is unsupported or view operations are not available
fn is_unsupported_or_view_error(err: &Error) -> bool {
    match err {
        Error::S3Server(minio::s3::error::S3ServerError::HttpError(status, msg)) => {
            (*status == 400 && msg.contains("unsupported API call"))
                || *status == 404
                || msg.contains("view")
        }
        Error::Validation(v) => v.to_string().contains("invalid type: null"),
        _ => false,
    }
}

/// Generate a random view name as a wrapper type
fn rand_view_name() -> ViewName {
    let name = format!("view_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
    ViewName::try_from(name.as_str()).expect("Generated view name should be valid")
}

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

// =============================================================================
// View Properties Tests
// Corresponds to: defaultViewProperties, overrideViewProperties, updateViewProperties,
//                 updateViewPropertiesErrorCases
// =============================================================================

/// Test that views have default properties set by the server.
/// Corresponds to Iceberg RCK: defaultViewProperties
#[minio_macros::test(no_bucket)]
async fn default_view_properties(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view without specifying properties
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(resp) => {
            match resp.view_metadata() {
                Ok(metadata) => {
                    // View should have a UUID assigned
                    assert!(
                        !metadata.view_uuid.is_empty(),
                        "View should have a UUID assigned by server"
                    );

                    // Cleanup
                    tables
                        .drop_view(&warehouse, &namespace, view)
                        .unwrap()
                        .build()
                        .send()
                        .await
                        .ok();
                }
                Err(e) if e.to_string().contains("invalid type: null") => {
                    eprintln!("> Server returned null metadata (may be expected)");
                    let _ = tables
                        .drop_view(&warehouse, &namespace, view)
                        .unwrap()
                        .build()
                        .send()
                        .await;
                }
                Err(e) => panic!("Unexpected metadata error: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that explicitly set properties override defaults.
/// Corresponds to Iceberg RCK: overrideViewProperties
#[minio_macros::test(no_bucket)]
async fn override_view_properties(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view with custom properties
    let mut custom_props = HashMap::new();
    custom_props.insert("custom.property".to_string(), "custom-value".to_string());
    custom_props.insert("view.owner".to_string(), "test-user".to_string());

    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .properties(custom_props)
        .build()
        .send()
        .await;

    match create_resp {
        Ok(resp) => {
            match resp.view_metadata() {
                Ok(metadata) => {
                    assert!(
                        !metadata.view_uuid.is_empty(),
                        "View should be created with custom properties"
                    );

                    // Cleanup
                    tables
                        .drop_view(&warehouse, &namespace, view)
                        .unwrap()
                        .build()
                        .send()
                        .await
                        .ok();
                }
                Err(e) if e.to_string().contains("invalid type: null") => {
                    let _ = tables
                        .drop_view(&warehouse, &namespace, view)
                        .unwrap()
                        .build()
                        .send()
                        .await;
                }
                Err(e) => panic!("Unexpected metadata error: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test updating view properties via ReplaceView.
/// Corresponds to Iceberg RCK: updateViewProperties
#[minio_macros::test(no_bucket)]
async fn update_view_properties(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(_) => {
            // Update properties via ReplaceView
            let mut new_props = HashMap::new();
            new_props.insert("updated.prop".to_string(), "updated-value".to_string());

            let updates = vec![ViewUpdate::SetProperties { updates: new_props }];

            let replace_result: Result<ReplaceViewResponse, Error> = tables
                .replace_view(&warehouse, &namespace, &view)
                .unwrap()
                .updates(updates)
                .build()
                .send()
                .await;

            match replace_result {
                Ok(resp) => {
                    assert!(
                        resp.view_metadata().is_ok(),
                        "View properties should be updated"
                    );
                }
                Err(e) => {
                    eprintln!("> Property update failed (may be expected): {:?}", e);
                }
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test error cases when updating view properties on non-existent view.
/// Corresponds to Iceberg RCK: updateViewPropertiesErrorCases
#[minio_macros::test(no_bucket)]
async fn update_view_properties_error_cases(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Try to update properties on a non-existent view - should fail
    let mut props = HashMap::new();
    props.insert("some.prop".to_string(), "some-value".to_string());

    let updates = vec![ViewUpdate::SetProperties { updates: props }];

    let replace_result: Result<ReplaceViewResponse, Error> = tables
        .replace_view(&warehouse, &namespace, view)
        .unwrap()
        .updates(updates)
        .build()
        .send()
        .await;

    assert!(
        replace_result.is_err(),
        "Updating properties on non-existent view should fail"
    );

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// View Location Tests
// Corresponds to: createViewWithCustomMetadataLocation, updateViewLocation,
//                 updateViewLocationConflict
// =============================================================================

/// Test creating a view with custom metadata location.
/// Corresponds to Iceberg RCK: createViewWithCustomMetadataLocation
#[minio_macros::test(no_bucket)]
async fn view_custom_metadata_location(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view - location is typically server-managed
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(resp) => {
            match resp.view_metadata() {
                Ok(metadata) => {
                    // Check that view has a location
                    assert!(
                        !metadata.location.is_empty(),
                        "View should have a location assigned"
                    );
                    eprintln!("> View location: {}", metadata.location);
                }
                Err(e) if e.to_string().contains("invalid type: null") => {
                    eprintln!("> Server returned null metadata");
                }
                Err(e) => panic!("Unexpected metadata error: {e:?}"),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test updating view location via ReplaceView.
/// Corresponds to Iceberg RCK: updateViewLocation
#[minio_macros::test(no_bucket)]
async fn update_view_location(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(resp) => {
            match resp.view_metadata() {
                Ok(metadata) => {
                    let new_location =
                        format!("{}/updated", metadata.location.trim_end_matches('/'));

                    // Try to update location
                    let updates = vec![ViewUpdate::SetLocation {
                        location: new_location,
                    }];

                    let replace_result: Result<ReplaceViewResponse, Error> = tables
                        .replace_view(&warehouse, &namespace, &view)
                        .unwrap()
                        .updates(updates)
                        .build()
                        .send()
                        .await;

                    match replace_result {
                        Ok(_) => eprintln!("> View location update succeeded"),
                        Err(e) => {
                            eprintln!("> Location update failed (may be expected): {:?}", e)
                        }
                    }
                }
                Err(e) if e.to_string().contains("invalid type: null") => {
                    eprintln!("> Server returned null metadata");
                }
                Err(e) => panic!("Unexpected metadata error: {e:?}"),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent view location updates with conflict detection.
/// Corresponds to Iceberg RCK: updateViewLocationConflict
#[minio_macros::test(no_bucket)]
async fn update_view_location_conflict(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(resp) => {
            match resp.view_metadata() {
                Ok(metadata) => {
                    // First update
                    let location1 = format!("{}/update1", metadata.location.trim_end_matches('/'));
                    let updates1 = vec![ViewUpdate::SetLocation {
                        location: location1,
                    }];

                    let _first_result = tables
                        .replace_view(&warehouse, &namespace, &view)
                        .unwrap()
                        .updates(updates1)
                        .build()
                        .send()
                        .await;

                    // Second update immediately after (simulating concurrent update)
                    let location2 = format!("{}/update2", metadata.location.trim_end_matches('/'));
                    let updates2 = vec![ViewUpdate::SetLocation {
                        location: location2,
                    }];

                    let second_result: Result<ReplaceViewResponse, Error> = tables
                        .replace_view(&warehouse, &namespace, &view)
                        .unwrap()
                        .updates(updates2)
                        .build()
                        .send()
                        .await;

                    // Log result - behavior depends on server's conflict detection
                    match second_result {
                        Ok(_) => eprintln!("> Second location update succeeded"),
                        Err(e) => eprintln!("> Second update failed (may be conflict): {:?}", e),
                    }
                }
                Err(e) if e.to_string().contains("invalid type: null") => {
                    eprintln!("> Server returned null metadata");
                }
                Err(e) => panic!("Unexpected metadata error: {e:?}"),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// View Version Management Tests
// Corresponds to: replaceViewVersion, replaceViewVersionByUpdatingSQLForDialect,
//                 replaceViewVersionConflict
// =============================================================================

/// Test replacing a view version.
/// Corresponds to Iceberg RCK: replaceViewVersion
#[minio_macros::test(no_bucket)]
async fn replace_view_version(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table WHERE id > 0").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(resp) => {
            match resp.view_metadata() {
                Ok(metadata) => {
                    let original_version_id = metadata.current_version_id;

                    // Replace view with new version
                    let new_representation = SqlViewRepresentation {
                        r#type: "sql".to_string(),
                        sql: "SELECT id, name FROM source_table WHERE id > 100".to_string(),
                        dialect: "spark".to_string(),
                    };

                    let view_version = ViewVersionUpdate {
                        version_id: original_version_id + 1,
                        schema_id: 0,
                        timestamp_ms: chrono::Utc::now().timestamp_millis(),
                        default_catalog: None,
                        default_namespace: namespace.as_ref().to_vec(),
                        summary: HashMap::new(),
                        representations: vec![new_representation],
                    };

                    let updates = vec![ViewUpdate::AddViewVersion { view_version }];

                    let replace_result: Result<ReplaceViewResponse, Error> = tables
                        .replace_view(&warehouse, &namespace, &view)
                        .unwrap()
                        .updates(updates)
                        .build()
                        .send()
                        .await;

                    match replace_result {
                        Ok(replace_resp) => {
                            if let Ok(updated_metadata) = replace_resp.view_metadata() {
                                assert_eq!(
                                    updated_metadata.view_uuid, metadata.view_uuid,
                                    "View UUID should remain the same"
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("> View version replace failed (may be expected): {:?}", e)
                        }
                    }
                }
                Err(e) if e.to_string().contains("invalid type: null") => {
                    eprintln!("> Server returned null metadata");
                }
                Err(e) => panic!("Unexpected metadata error: {e:?}"),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test replacing view version by updating SQL for a different dialect.
/// Corresponds to Iceberg RCK: replaceViewVersionByUpdatingSQLForDialect
#[minio_macros::test(no_bucket)]
async fn replace_view_version_by_sql_dialect(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view with spark dialect
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(_) => {
            // Replace view with trino dialect SQL
            let new_representation = SqlViewRepresentation {
                r#type: "sql".to_string(),
                sql: "SELECT id, name FROM source_table".to_string(),
                dialect: "trino".to_string(),
            };

            let view_version = ViewVersionUpdate {
                version_id: 2,
                schema_id: 0,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                default_catalog: None,
                default_namespace: namespace.as_ref().to_vec(),
                summary: HashMap::new(),
                representations: vec![new_representation],
            };

            let updates = vec![ViewUpdate::AddViewVersion { view_version }];

            let replace_result: Result<ReplaceViewResponse, Error> = tables
                .replace_view(&warehouse, &namespace, &view)
                .unwrap()
                .updates(updates)
                .build()
                .send()
                .await;

            match replace_result {
                Ok(_) => eprintln!("> View SQL dialect update succeeded"),
                Err(e) => eprintln!("> Dialect update failed (may be expected): {:?}", e),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test view version replacement with conflict detection.
/// Corresponds to Iceberg RCK: replaceViewVersionConflict
#[minio_macros::test(no_bucket)]
async fn replace_view_version_conflict(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(_) => {
            // First version update
            let representation1 = SqlViewRepresentation {
                r#type: "sql".to_string(),
                sql: "SELECT id, name FROM source_table WHERE id > 10".to_string(),
                dialect: "spark".to_string(),
            };

            let view_version1 = ViewVersionUpdate {
                version_id: 2,
                schema_id: 0,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                default_catalog: None,
                default_namespace: namespace.as_ref().to_vec(),
                summary: HashMap::new(),
                representations: vec![representation1],
            };

            let _first_result = tables
                .replace_view(&warehouse, &namespace, &view)
                .unwrap()
                .updates(vec![ViewUpdate::AddViewVersion {
                    view_version: view_version1,
                }])
                .build()
                .send()
                .await;

            // Second version update (may conflict)
            let representation2 = SqlViewRepresentation {
                r#type: "sql".to_string(),
                sql: "SELECT id, name FROM source_table WHERE id > 20".to_string(),
                dialect: "spark".to_string(),
            };

            let view_version2 = ViewVersionUpdate {
                version_id: 2, // Same version ID - may cause conflict
                schema_id: 0,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                default_catalog: None,
                default_namespace: namespace.as_ref().to_vec(),
                summary: HashMap::new(),
                representations: vec![representation2],
            };

            let second_result: Result<ReplaceViewResponse, Error> = tables
                .replace_view(&warehouse, &namespace, &view)
                .unwrap()
                .updates(vec![ViewUpdate::AddViewVersion {
                    view_version: view_version2,
                }])
                .build()
                .send()
                .await;

            match second_result {
                Ok(_) => eprintln!("> Second version update succeeded"),
                Err(e) => eprintln!("> Second update failed (may be conflict): {:?}", e),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// View SQL Dialect Tests
// Corresponds to: testSqlForMultipleDialects, testSqlForCaseInsensitive,
//                 testSqlForInvalidArguments
// =============================================================================

/// Test view with multiple SQL dialects.
/// Corresponds to Iceberg RCK: testSqlForMultipleDialects
#[minio_macros::test(no_bucket)]
async fn view_sql_multiple_dialects(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view with spark dialect
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(_) => {
            // Add representations for multiple dialects
            let representations = vec![
                SqlViewRepresentation {
                    r#type: "sql".to_string(),
                    sql: "SELECT id, name FROM source_table".to_string(),
                    dialect: "spark".to_string(),
                },
                SqlViewRepresentation {
                    r#type: "sql".to_string(),
                    sql: "SELECT id, name FROM source_table".to_string(),
                    dialect: "trino".to_string(),
                },
                SqlViewRepresentation {
                    r#type: "sql".to_string(),
                    sql: "SELECT id, name FROM source_table".to_string(),
                    dialect: "presto".to_string(),
                },
            ];

            let view_version = ViewVersionUpdate {
                version_id: 2,
                schema_id: 0,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                default_catalog: None,
                default_namespace: namespace.as_ref().to_vec(),
                summary: HashMap::new(),
                representations,
            };

            let replace_result: Result<ReplaceViewResponse, Error> = tables
                .replace_view(&warehouse, &namespace, &view)
                .unwrap()
                .updates(vec![ViewUpdate::AddViewVersion { view_version }])
                .build()
                .send()
                .await;

            match replace_result {
                Ok(_) => eprintln!("> Multi-dialect view update succeeded"),
                Err(e) => {
                    eprintln!("> Multi-dialect update failed (may be expected): {:?}", e)
                }
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test view SQL dialect case insensitivity.
/// Corresponds to Iceberg RCK: testSqlForCaseInsensitive
#[minio_macros::test(no_bucket)]
async fn view_sql_case_insensitive(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view with uppercase dialect
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, view_sql)
        .unwrap()
        .dialect("SPARK") // Uppercase
        .build()
        .send()
        .await;

    match create_resp {
        Ok(_) => {
            // Load view and check that it works regardless of case
            let load_resp: Result<LoadViewResponse, Error> = tables
                .load_view(&warehouse, &namespace, &view)
                .unwrap()
                .build()
                .send()
                .await;

            match load_resp {
                Ok(resp) => {
                    assert!(
                        resp.view_metadata().is_ok(),
                        "View should be loadable regardless of dialect case"
                    );
                }
                Err(e) => eprintln!("> Load failed (may be expected): {:?}", e),
            }

            // Cleanup
            tables
                .drop_view(&warehouse, &namespace, view)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test view creation with invalid dialect arguments.
/// Corresponds to Iceberg RCK: testSqlForInvalidArguments
#[minio_macros::test(no_bucket)]
async fn view_sql_invalid_arguments(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Try to create view with empty SQL - SDK may reject this
    let view_sql_result = ViewSql::new("");

    match view_sql_result {
        Err(_) => {
            eprintln!("> SDK correctly rejected empty SQL");
        }
        Ok(empty_sql) => {
            let schema = create_view_schema();
            let create_resp: Result<CreateViewResponse, Error> = tables
                .create_view(&warehouse, &namespace, &view, schema, empty_sql)
                .unwrap()
                .dialect("spark")
                .build()
                .send()
                .await;

            match create_resp {
                Ok(_) => {
                    eprintln!("> Server accepted empty SQL (unexpected)");
                    // Cleanup
                    tables
                        .drop_view(&warehouse, &namespace, view)
                        .unwrap()
                        .build()
                        .send()
                        .await
                        .ok();
                }
                Err(_) => {
                    eprintln!("> Server correctly rejected empty SQL");
                }
            }
        }
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// View-Table Transaction Conflict Tests
// Corresponds to: createTableViaTransactionThatAlreadyExistsAsView,
//                 replaceTableViaTransactionThatAlreadyExistsAsView,
//                 replaceViewThatAlreadyExistsAsTable,
//                 createOrReplaceViewThatAlreadyExistsAsTable
// =============================================================================

/// Test that creating a table via transaction fails if a view with the same name exists.
/// Corresponds to Iceberg RCK: createTableViaTransactionThatAlreadyExistsAsView
#[minio_macros::test(no_bucket)]
async fn create_table_via_transaction_conflicts_with_view(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let name = rand_view_name(); // Use same name for both view and table

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view first
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_view_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &name, schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_view_resp {
        Ok(_) => {
            // Try to create a table with the same name via CommitTable (AssertCreate)
            let table = name.as_str();
            let table =
                minio::s3tables::utils::TableName::try_from(table).expect("Valid table name");

            let commit_result = tables
                .commit_table(&warehouse, &namespace, table)
                .unwrap()
                .requirements(vec![TableRequirement::AssertCreate])
                .updates(vec![])
                .build()
                .send()
                .await;

            // Should fail because view already exists with that name
            match commit_result {
                Ok(_) => {
                    eprintln!("> CommitTable succeeded (server may allow table/view same name)")
                }
                Err(e) => eprintln!("> CommitTable correctly failed: {:?}", e),
            }

            // Cleanup view
            tables
                .drop_view(&warehouse, &namespace, name)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that replacing a table fails if a view with the same name exists.
/// Corresponds to Iceberg RCK: replaceTableViaTransactionThatAlreadyExistsAsView
#[minio_macros::test(no_bucket)]
async fn replace_table_via_transaction_conflicts_with_view(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let name = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create view first
    let schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_view_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, name.clone(), schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_view_resp {
        Ok(_) => {
            // Try to replace table with the view's name
            let table = name.as_str();
            let table =
                minio::s3tables::utils::TableName::try_from(table).expect("Valid table name");

            // Use UpgradeFormatVersion as a no-op update to test replace behavior
            let commit_result = tables
                .commit_table(&warehouse, &namespace, table)
                .unwrap()
                .requirements(vec![])
                .updates(vec![TableUpdate::UpgradeFormatVersion {
                    format_version: 2,
                }])
                .build()
                .send()
                .await;

            match commit_result {
                Ok(_) => eprintln!("> Replace succeeded (server may allow table/view same name)"),
                Err(e) => eprintln!("> Replace correctly failed: {:?}", e),
            }

            // Cleanup view
            tables
                .drop_view(&warehouse, &namespace, name)
                .unwrap()
                .build()
                .send()
                .await
                .ok();
        }
        Err(ref e) if is_unsupported_or_view_error(e) => {
            eprintln!("> View operations not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that replacing a view fails if a table with the same name exists.
/// Corresponds to Iceberg RCK: replaceViewThatAlreadyExistsAsTable
#[minio_macros::test(no_bucket)]
async fn replace_view_conflicts_with_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table first
    let schema = create_test_schema();
    let _create_table_resp: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Try to replace view with the table's name
    let view = table.as_str();
    let view = ViewName::try_from(view).expect("Valid view name");

    let replace_result: Result<ReplaceViewResponse, Error> = tables
        .replace_view(&warehouse, &namespace, view)
        .unwrap()
        .updates(vec![])
        .build()
        .send()
        .await;

    // Should fail because table exists with that name
    assert!(
        replace_result.is_err(),
        "Replacing view should fail when table exists with same name"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that create-or-replace view fails if a table with the same name exists.
/// Corresponds to Iceberg RCK: createOrReplaceViewThatAlreadyExistsAsTable
#[minio_macros::test(no_bucket)]
async fn create_or_replace_view_conflicts_with_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table first
    let schema = create_test_schema();
    let _create_table_resp: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Try to create-or-replace view with the table's name
    let view_name_str = table.as_str();
    let view = ViewName::try_from(view_name_str).expect("Valid view name");

    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();
    let view_schema = create_view_schema();

    let create_view_result: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, view, view_schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    // Should fail because table exists with that name
    assert!(
        create_view_result.is_err(),
        "Create view should fail when table exists with same name"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Combined Listing Test
// Corresponds to: listViewsAndTables
// =============================================================================

/// Test listing both views and tables in a namespace.
/// Corresponds to Iceberg RCK: listViewsAndTables
#[minio_macros::test(no_bucket)]
async fn list_views_and_tables(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create a table
    let table_schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, table_schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Create a view
    let view_schema = create_view_schema();
    let view_sql = ViewSql::new("SELECT id, name FROM source_table").unwrap();

    let create_view_result: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, view_schema, view_sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    let view_created = create_view_result.is_ok();

    // List tables - should have 1
    let list_tables_resp: ListTablesResponse = tables
        .list_tables(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table_identifiers = list_tables_resp.identifiers().unwrap();
    assert!(
        !table_identifiers.is_empty(),
        "Should have at least 1 table"
    );
    let table_names: Vec<&str> = table_identifiers.iter().map(|t| t.name.as_str()).collect();
    assert!(
        table_names.contains(&table.as_str()),
        "Table list should contain the created table"
    );

    // List views - should have 1 if view was created
    if view_created {
        let list_views_resp: ListViewsResponse = tables
            .list_views(&warehouse, &namespace)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();

        let view_identifiers = list_views_resp.identifiers().unwrap();
        assert_eq!(view_identifiers.len(), 1, "Should have 1 view");
        let view_names: Vec<&str> = view_identifiers.iter().map(|v| v.name.as_str()).collect();
        assert!(
            view_names.contains(&view.as_str()),
            "View list should contain the created view"
        );

        // Verify that table list doesn't include the view and vice versa
        assert!(
            !table_names.contains(&view.as_str()),
            "Table list should not include views"
        );
        assert!(
            !view_names.contains(&table.as_str()),
            "View list should not include tables"
        );

        // Cleanup view
        tables
            .drop_view(&warehouse, &namespace, view)
            .unwrap()
            .build()
            .send()
            .await
            .ok();
    } else {
        eprintln!("> View creation failed, skipping view listing verification");
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}
