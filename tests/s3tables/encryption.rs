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

//! Integration tests for encryption operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::HasEncryptionConfiguration;
use minio::s3tables::types::{EncryptionConfiguration, SseAlgorithm};
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

/// Test getting warehouse encryption configuration
#[minio_macros::test(no_bucket)]
async fn get_warehouse_encryption(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Get encryption config
    let resp = tables
        .get_warehouse_encryption(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.encryption_configuration().unwrap();
            println!(
                "> Warehouse encryption algorithm: {:?}",
                config.sse_algorithm()
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse encryption API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting warehouse encryption configuration
#[minio_macros::test(no_bucket)]
async fn put_warehouse_encryption(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Set S3-managed encryption
    let encryption = EncryptionConfiguration::s3_managed();

    let resp = tables
        .put_warehouse_encryption(&warehouse, encryption)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Warehouse encryption set successfully");

            // Verify by getting the encryption config
            let get_resp = tables
                .get_warehouse_encryption(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match get_resp {
                Ok(resp) => {
                    let config = resp.encryption_configuration().unwrap();
                    assert_eq!(
                        *config.sse_algorithm(),
                        SseAlgorithm::Aes256,
                        "Should be AES256"
                    );
                }
                Err(e) => {
                    eprintln!("> Failed to get encryption after put: {e:?}");
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse encryption API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test deleting warehouse encryption configuration
#[minio_macros::test(no_bucket)]
async fn delete_warehouse_encryption(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // First set encryption
    let encryption = EncryptionConfiguration::s3_managed();

    let put_resp = tables
        .put_warehouse_encryption(&warehouse, encryption)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the encryption config
            let del_resp = tables
                .delete_warehouse_encryption(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Warehouse encryption deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Warehouse encryption API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting encryption: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse encryption API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting encryption: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test getting table encryption configuration
#[minio_macros::test(no_bucket)]
async fn get_table_encryption(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get encryption config
    let resp = tables
        .get_table_encryption(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.encryption_configuration().unwrap();
            println!("> Table encryption algorithm: {:?}", config.sse_algorithm());
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table encryption API not supported by server, skipping test");
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

/// Test putting table encryption configuration
#[minio_macros::test(no_bucket)]
async fn put_table_encryption(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Set S3-managed encryption
    let encryption = EncryptionConfiguration::s3_managed();

    let resp = tables
        .put_table_encryption(&warehouse, &namespace, &table, encryption)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Table encryption set successfully");

            // Verify by getting the encryption config
            let get_resp = tables
                .get_table_encryption(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            match get_resp {
                Ok(resp) => {
                    let config = resp.encryption_configuration().unwrap();
                    assert_eq!(
                        *config.sse_algorithm(),
                        SseAlgorithm::Aes256,
                        "Should be AES256"
                    );
                }
                Err(e) => {
                    eprintln!("> Failed to get encryption after put: {e:?}");
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table encryption API not supported by server, skipping test");
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

/// Test deleting table encryption configuration
#[minio_macros::test(no_bucket)]
async fn delete_table_encryption(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // First set encryption
    let encryption = EncryptionConfiguration::s3_managed();

    let put_resp = tables
        .put_table_encryption(&warehouse, &namespace, &table, encryption)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the encryption config
            let del_resp = tables
                .delete_table_encryption(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Table encryption deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Table encryption API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting encryption: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table encryption API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting encryption: {e:?}"),
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
