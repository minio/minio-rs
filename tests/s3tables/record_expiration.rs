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

//! Integration tests for record expiration operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::{HasExpirationConfiguration, HasExpirationJobStatus};
use minio::s3tables::types::RecordExpirationConfiguration;
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

/// Test getting table expiration configuration
#[minio_macros::test(no_bucket)]
async fn get_table_expiration(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get expiration config
    let resp = tables
        .get_table_expiration(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.expiration_configuration().unwrap();
            println!("> Table expiration config: {:?}", config);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table expiration API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchExpiration") {
                println!("> No expiration config exists (expected for new table)");
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

/// Test putting table expiration configuration
#[minio_macros::test(no_bucket)]
async fn put_table_expiration(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Create an expiration configuration
    // The "expiration_timestamp" is a hypothetical column name
    let config = RecordExpirationConfiguration::enabled("expiration_timestamp");

    let resp = tables
        .put_table_expiration(&warehouse, &namespace, &table, config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Table expiration set successfully");

            // Verify by getting the config
            let get_resp = tables
                .get_table_expiration(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            if let Ok(resp) = get_resp {
                let config = resp.expiration_configuration().unwrap();
                assert!(config.is_enabled(), "Expiration should be enabled");
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table expiration API not supported by server, skipping test");
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

/// Test getting table expiration job status
#[minio_macros::test(no_bucket)]
async fn get_table_expiration_job_status(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get expiration job status
    let resp = tables
        .get_table_expiration_job_status(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let status = resp.expiration_job_status().unwrap();
            println!("> Table expiration job status: {:?}", status);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table expiration job status API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchJob") {
                println!("> No expiration job exists (expected)");
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

/// Test enabling and disabling record expiration
#[minio_macros::test(no_bucket)]
async fn toggle_table_expiration(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Enable expiration
    let enabled_config = RecordExpirationConfiguration::enabled("expiration_timestamp");

    let resp = tables
        .put_table_expiration(&warehouse, &namespace, &table, enabled_config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Expiration enabled");

            // Disable expiration
            let disabled_config = RecordExpirationConfiguration::disabled();

            let disable_resp = tables
                .put_table_expiration(&warehouse, &namespace, &table, disabled_config)
                .unwrap()
                .build()
                .send()
                .await;

            match disable_resp {
                Ok(_) => {
                    println!("> Expiration disabled");

                    // Verify
                    let get_resp = tables
                        .get_table_expiration(&warehouse, &namespace, &table)
                        .unwrap()
                        .build()
                        .send()
                        .await;

                    if let Ok(resp) = get_resp {
                        let config = resp.expiration_configuration().unwrap();
                        assert!(!config.is_enabled(), "Expiration should be disabled");
                    }
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Table expiration API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error disabling expiration: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table expiration API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error enabling expiration: {e:?}"),
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
