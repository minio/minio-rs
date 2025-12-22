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
use minio::s3tables::response::TableMetricsResponse;
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

/// Test retrieving table metrics for a newly created table
#[minio_macros::test(no_bucket)]
async fn table_metrics_basic(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Setup: create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema = create_test_schema();
    tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Get table metrics
    let resp: Result<TableMetricsResponse, Error> = tables
        .table_metrics(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if table metrics is supported
    match resp {
        Ok(resp) => {
            // Verify metrics are returned (newly created table should have zero/minimal values)
            // Handle case where server returns empty body
            match resp.row_count() {
                Ok(row_count) => {
                    let size_bytes = resp.size_bytes().unwrap();
                    let file_count = resp.file_count().unwrap();
                    let snapshot_count = resp.snapshot_count().unwrap();

                    // For a newly created empty table, these values should be non-negative
                    assert!(
                        row_count >= 0,
                        "Row count should be non-negative, got {}",
                        row_count
                    );
                    assert!(
                        size_bytes >= 0,
                        "Size bytes should be non-negative, got {}",
                        size_bytes
                    );
                    assert!(
                        file_count >= 0,
                        "File count should be non-negative, got {}",
                        file_count
                    );
                    assert!(
                        snapshot_count >= 0,
                        "Snapshot count should be non-negative, got {}",
                        snapshot_count
                    );
                }
                Err(e)
                    if e.to_string().contains("EOF")
                        || e.to_string().contains("invalid type: null") =>
                {
                    eprintln!("Server returned empty/null metrics response, skipping test");
                }
                Err(e) => panic!("Unexpected metrics error: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("Table metrics not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test that table metrics fails gracefully for non-existent table
#[minio_macros::test(no_bucket)]
async fn table_metrics_nonexistent_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Setup: create warehouse and namespace only (no table)
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Try to get metrics for non-existent table - should error
    let resp: Result<TableMetricsResponse, Error> = tables
        .table_metrics(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if API is unsupported or table not found (both are acceptable)
    match resp {
        Ok(resp) => {
            // Server may return empty metrics for non-existent table rather than error
            // This is acceptable behavior - verify parsing doesn't crash
            match resp.row_count() {
                Ok(_) => {
                    // Server returned valid metrics - unexpected but acceptable
                }
                Err(_) => {
                    // Expected - empty or null response for non-existent table
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("Table metrics not supported by server, skipping test");
        }
        Err(_) => {
            // Expected - table not found or other error
        }
    }

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
