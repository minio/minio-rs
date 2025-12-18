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

//! Integration tests for warehouse metrics operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::HasMetricsConfiguration;
use minio::s3tables::types::MetricsConfiguration;
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

/// Test getting warehouse metrics configuration
#[minio_macros::test(no_bucket)]
async fn get_warehouse_metrics(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Get metrics config
    let resp = tables
        .get_warehouse_metrics(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.metrics_configuration().unwrap();
            println!("> Warehouse metrics config: {:?}", config);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse metrics API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchMetrics") {
                println!("> No metrics config exists (expected for new warehouse)");
            } else {
                panic!("Unexpected error: {e:?}");
            }
        }
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting warehouse metrics configuration
#[minio_macros::test(no_bucket)]
async fn put_warehouse_metrics(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Create a metrics configuration
    let config = MetricsConfiguration::enabled();

    let resp = tables
        .put_warehouse_metrics(&warehouse, config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Warehouse metrics set successfully");

            // Verify by getting the config
            let get_resp = tables
                .get_warehouse_metrics(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            if let Ok(resp) = get_resp {
                let config = resp.metrics_configuration().unwrap();
                assert!(config.is_enabled(), "Metrics should be enabled");
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse metrics API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test deleting warehouse metrics configuration
#[minio_macros::test(no_bucket)]
async fn delete_warehouse_metrics(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // First set metrics
    let config = MetricsConfiguration::enabled();

    let put_resp = tables
        .put_warehouse_metrics(&warehouse, config)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the metrics config
            let del_resp = tables
                .delete_warehouse_metrics(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Warehouse metrics deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Warehouse metrics API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting metrics: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse metrics API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting metrics: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test enabling and disabling metrics
#[minio_macros::test(no_bucket)]
async fn toggle_warehouse_metrics(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Enable metrics
    let enabled_config = MetricsConfiguration::enabled();

    let resp = tables
        .put_warehouse_metrics(&warehouse, enabled_config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Metrics enabled");

            // Disable metrics
            let disabled_config = MetricsConfiguration::disabled();

            let disable_resp = tables
                .put_warehouse_metrics(&warehouse, disabled_config)
                .unwrap()
                .build()
                .send()
                .await;

            match disable_resp {
                Ok(_) => {
                    println!("> Metrics disabled");

                    // Verify
                    let get_resp = tables
                        .get_warehouse_metrics(&warehouse)
                        .unwrap()
                        .build()
                        .send()
                        .await;

                    if let Ok(resp) = get_resp {
                        let config = resp.metrics_configuration().unwrap();
                        assert!(!config.is_enabled(), "Metrics should be disabled");
                    }
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Warehouse metrics API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error disabling metrics: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse metrics API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error enabling metrics: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}
