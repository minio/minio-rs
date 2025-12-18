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

//! Integration tests for replication operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::{HasReplicationConfiguration, HasReplicationStatus};
use minio::s3tables::types::{ReplicationConfiguration, ReplicationRule};
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

/// Test getting warehouse replication configuration
#[minio_macros::test(no_bucket)]
async fn get_warehouse_replication(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Get replication config
    let resp = tables
        .get_warehouse_replication(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.replication_configuration().unwrap();
            println!("> Warehouse replication rules: {:?}", config.rules);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse replication API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchReplication") {
                println!("> No replication config exists (expected for new warehouse)");
            } else {
                panic!("Unexpected error: {e:?}");
            }
        }
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting warehouse replication configuration
#[minio_macros::test(no_bucket)]
async fn put_warehouse_replication(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Create a replication configuration
    let config = ReplicationConfiguration::new(vec![ReplicationRule::new(
        "arn:aws:s3tables:us-west-2:123456789012:bucket/dest-bucket",
    )]);

    let resp = tables
        .put_warehouse_replication(&warehouse, config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Warehouse replication set successfully");

            // Verify by getting the config
            let get_resp = tables
                .get_warehouse_replication(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            if let Ok(resp) = get_resp {
                let config = resp.replication_configuration().unwrap();
                assert!(!config.rules.is_empty(), "Should have replication rules");
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse replication API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test deleting warehouse replication configuration
#[minio_macros::test(no_bucket)]
async fn delete_warehouse_replication(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // First set replication
    let config = ReplicationConfiguration::new(vec![ReplicationRule::new(
        "arn:aws:s3tables:us-west-2:123456789012:bucket/dest-bucket",
    )]);

    let put_resp = tables
        .put_warehouse_replication(&warehouse, config)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the replication config
            let del_resp = tables
                .delete_warehouse_replication(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Warehouse replication deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Warehouse replication API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting replication: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse replication API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting replication: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test getting table replication configuration
#[minio_macros::test(no_bucket)]
async fn get_table_replication(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get replication config
    let resp = tables
        .get_table_replication(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.replication_configuration().unwrap();
            println!("> Table replication rules: {:?}", config.rules);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table replication API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchReplication") {
                println!("> No replication config exists (expected for new table)");
            } else {
                panic!("Unexpected error: {e:?}");
            }
        }
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

/// Test putting table replication configuration
#[minio_macros::test(no_bucket)]
async fn put_table_replication(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Create a replication configuration
    let config = ReplicationConfiguration::new(vec![ReplicationRule::new(
        "arn:aws:s3tables:us-west-2:123456789012:bucket/dest-bucket",
    )]);

    let resp = tables
        .put_table_replication(&warehouse, &namespace, &table, config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Table replication set successfully");
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table replication API not supported by server, skipping test");
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

/// Test deleting table replication configuration
#[minio_macros::test(no_bucket)]
async fn delete_table_replication(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // First set replication
    let config = ReplicationConfiguration::new(vec![ReplicationRule::new(
        "arn:aws:s3tables:us-west-2:123456789012:bucket/dest-bucket",
    )]);

    let put_resp = tables
        .put_table_replication(&warehouse, &namespace, &table, config)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the replication config
            let del_resp = tables
                .delete_table_replication(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Table replication deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Table replication API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting replication: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table replication API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting replication: {e:?}"),
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

/// Test getting table replication status
#[minio_macros::test(no_bucket)]
async fn get_table_replication_status(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get replication status
    let resp = tables
        .get_table_replication_status(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let status = resp.replication_status().unwrap();
            println!("> Table replication status: {:?}", status);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table replication status API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchReplication") {
                println!("> No replication config exists (expected)");
            } else {
                panic!("Unexpected error: {e:?}");
            }
        }
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
