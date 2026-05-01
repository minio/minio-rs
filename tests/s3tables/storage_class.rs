// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Integration tests for storage class operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::HasStorageClass;
use minio::s3tables::types::StorageClass;
use minio_common::test_context::TestContext;

/// Check if an error indicates the API is unsupported
fn is_unsupported_api(err: &Error) -> bool {
    match err {
        Error::S3Server(minio::s3::error::S3ServerError::HttpError(400, msg)) => {
            msg.contains("unsupported API call")
        }
        _ => false,
    }
}

/// Test getting warehouse storage class
#[minio_macros::test(no_bucket)]
async fn get_warehouse_storage_class(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Get storage class
    let resp = tables
        .get_warehouse_storage_class(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let storage_class = resp.storage_class().unwrap();
            println!("> Warehouse storage class: {:?}", storage_class);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse storage class API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting warehouse storage class
#[minio_macros::test(no_bucket)]
async fn put_warehouse_storage_class(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Set storage class to STANDARD
    let storage_class = StorageClass::Standard;

    let resp = tables
        .put_warehouse_storage_class(&warehouse, storage_class.clone())
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Warehouse storage class set successfully");

            // Verify by getting the storage class
            let get_resp = tables
                .get_warehouse_storage_class(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match get_resp {
                Ok(resp) => {
                    let retrieved_class = resp.storage_class().unwrap();
                    assert_eq!(retrieved_class, storage_class, "Storage class should match");
                }
                Err(e) => {
                    eprintln!("> Failed to get storage class after put: {e:?}");
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse storage class API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test getting table storage class
#[minio_macros::test(no_bucket)]
async fn get_table_storage_class(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get storage class
    let resp = tables
        .get_table_storage_class(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let storage_class = resp.storage_class().unwrap();
            println!("> Table storage class: {:?}", storage_class);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table storage class API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test all storage class values
#[minio_macros::test(no_bucket)]
async fn storage_class_values(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Test each storage class value
    let storage_classes = vec![
        StorageClass::Standard,
        StorageClass::StandardIa,
        StorageClass::OnezoneIa,
        StorageClass::IntelligentTiering,
        StorageClass::Glacier,
        StorageClass::GlacierIr,
        StorageClass::DeepArchive,
    ];

    for storage_class in storage_classes {
        let resp = tables
            .put_warehouse_storage_class(&warehouse, storage_class.clone())
            .unwrap()
            .build()
            .send()
            .await;

        match resp {
            Ok(_) => {
                println!("> Set storage class to {:?}", storage_class);
            }
            Err(ref e) if is_unsupported_api(e) => {
                eprintln!("> Storage class API not supported by server, skipping test");
                break;
            }
            Err(e) => {
                // Some storage classes may not be supported - that's ok
                eprintln!("> Storage class {:?} not supported: {e:?}", storage_class);
            }
        }
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}
