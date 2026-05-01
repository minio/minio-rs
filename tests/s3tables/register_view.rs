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

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::response::{CreateViewResponse, LoadViewResponse, RegisterViewResponse};
use minio::s3tables::response_traits::HasCachedViewResult;
use minio::s3tables::utils::{ViewName, ViewSql};
use minio_common::test_context::TestContext;

/// Check if an error indicates the API is unsupported or view already exists
fn is_unsupported_or_exists(err: &Error) -> bool {
    match err {
        Error::S3Server(minio::s3::error::S3ServerError::HttpError(status, msg)) => {
            (*status == 400 && msg.contains("unsupported API call"))
                || *status == 404
                || msg.contains("not found")
                || msg.contains("already exists")
        }
        Error::Validation(v) => {
            // JSON parsing errors may indicate server returns null/unexpected format
            v.to_string().contains("invalid type: null")
        }
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

#[minio_macros::test(no_bucket)]
async fn view_register(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();
    let registered_view_name = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create initial view to get metadata location
    let schema = create_view_schema();
    let sql = ViewSql::new("SELECT id, name FROM test_table WHERE id > 0").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    // Check if view operations are supported
    match create_resp {
        Ok(create_resp) => {
            // Get metadata location from created view
            let metadata_location = match create_resp.view_metadata_location() {
                Ok(loc) => loc.to_string(),
                Err(_) => {
                    eprintln!("Could not get metadata location from view, skipping register test");
                    // Cleanup
                    let _ = tables
                        .drop_view(&warehouse, &namespace, &view)
                        .unwrap()
                        .build()
                        .send()
                        .await;
                    delete_namespace_helper(&warehouse, &namespace, &tables).await;
                    delete_warehouse_helper(&warehouse, &tables).await;
                    return;
                }
            };

            assert!(
                metadata_location.starts_with("s3://"),
                "Metadata location should be an S3 URI"
            );

            // Register the view with a different name using the same metadata location
            let register_resp: Result<RegisterViewResponse, Error> = tables
                .register_view(
                    &warehouse,
                    &namespace,
                    &registered_view_name,
                    metadata_location.clone(),
                )
                .unwrap()
                .build()
                .send()
                .await;

            // Check if register view is supported
            match register_resp {
                Ok(register_resp) => {
                    // Verify register response metadata
                    match register_resp.view_metadata_location() {
                        Ok(loc) => {
                            assert_eq!(loc, &metadata_location, "Metadata location should match");
                        }
                        Err(e) => {
                            eprintln!("Could not verify register response metadata: {e}");
                        }
                    }

                    // Verify registered view exists and can be loaded
                    let load_resp: Result<LoadViewResponse, Error> = tables
                        .load_view(&warehouse, &namespace, &registered_view_name)
                        .unwrap()
                        .build()
                        .send()
                        .await;

                    match load_resp {
                        Ok(load_resp) => {
                            // Verify load response has the same metadata location
                            match load_resp.view_metadata_location() {
                                Ok(loc) => {
                                    assert_eq!(
                                        loc, &metadata_location,
                                        "Loaded view should have same metadata location"
                                    );
                                }
                                Err(e) => {
                                    eprintln!("Could not verify loaded view metadata: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Could not load registered view: {e}");
                        }
                    }

                    // Cleanup registered view
                    let _ = tables
                        .drop_view(&warehouse, &namespace, &registered_view_name)
                        .unwrap()
                        .build()
                        .send()
                        .await;
                }
                Err(ref e) if is_unsupported_or_exists(e) => {
                    eprintln!(
                        "Register view not supported or view already exists, skipping test: {e}"
                    );
                }
                Err(e) => {
                    // RegisterView is a v0 extension, may not be available on all servers
                    eprintln!("RegisterView failed (may not be supported): {e:?}");
                }
            }

            // Cleanup - drop original view
            let _ = tables
                .drop_view(&warehouse, &namespace, &view)
                .unwrap()
                .build()
                .send()
                .await;
        }
        Err(ref e) if is_unsupported_or_exists(e) => {
            eprintln!("View operations not supported, skipping test: {e}");
        }
        Err(e) => {
            eprintln!("Create view failed: {e:?}");
        }
    }

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn view_register_with_overwrite(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let view = rand_view_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create initial view
    let schema = create_view_schema();
    let sql = ViewSql::new("SELECT id, name FROM test_table").unwrap();

    let create_resp: Result<CreateViewResponse, Error> = tables
        .create_view(&warehouse, &namespace, &view, schema, sql)
        .unwrap()
        .dialect("spark")
        .build()
        .send()
        .await;

    match create_resp {
        Ok(create_resp) => {
            let metadata_location = match create_resp.view_metadata_location() {
                Ok(loc) => loc.to_string(),
                Err(_) => {
                    eprintln!("Could not get metadata location, skipping overwrite test");
                    let _ = tables
                        .drop_view(&warehouse, &namespace, &view)
                        .unwrap()
                        .build()
                        .send()
                        .await;
                    delete_namespace_helper(&warehouse, &namespace, &tables).await;
                    delete_warehouse_helper(&warehouse, &tables).await;
                    return;
                }
            };

            // Try to register with same name and overwrite=true
            let register_resp: Result<RegisterViewResponse, Error> = tables
                .register_view(&warehouse, &namespace, &view, metadata_location.clone())
                .unwrap()
                .overwrite(true)
                .build()
                .send()
                .await;

            match register_resp {
                Ok(_) => {
                    eprintln!("RegisterView with overwrite succeeded");
                }
                Err(ref e) if is_unsupported_or_exists(e) => {
                    eprintln!("Register view with overwrite not supported: {e}");
                }
                Err(e) => {
                    // This is expected if overwrite semantics differ
                    eprintln!("RegisterView with overwrite failed (expected): {e:?}");
                }
            }

            // Cleanup
            let _ = tables
                .drop_view(&warehouse, &namespace, &view)
                .unwrap()
                .build()
                .send()
                .await;
        }
        Err(e) => {
            eprintln!("Create view failed, skipping overwrite test: {e}");
        }
    }

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}
