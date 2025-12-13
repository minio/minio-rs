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
use minio::s3tables::filter::FilterBuilder;
use minio::s3tables::response::{PlanTableScanResponse, PlanningStatus};
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

/// Test submitting a scan plan for a table
#[minio_macros::test(no_bucket)]
async fn plan_table_scan_basic(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Submit a scan plan
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            // Verify the response
            let result = resp.result().unwrap();
            assert!(
                result.status == PlanningStatus::Completed
                    || result.status == PlanningStatus::Submitted,
                "Should return completed or submitted status"
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test submitting a scan plan with select fields
#[minio_macros::test(no_bucket)]
async fn plan_table_scan_with_select(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Submit a scan plan with field selection
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .select(vec!["id".to_string()])
        .case_sensitive(true)
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            // Verify the response
            let result = resp.result().unwrap();
            assert!(
                result.status == PlanningStatus::Completed
                    || result.status == PlanningStatus::Submitted,
                "Should return completed or submitted status"
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test fetching planning result for a submitted plan
#[minio_macros::test(no_bucket)]
async fn fetch_planning_result(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Submit a scan plan
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();

            // If a plan_id was returned (async planning), try to fetch the result
            if let Some(plan_id) = result.plan_id {
                let fetch_resp = tables
                    .fetch_planning_result(
                        warehouse_name.clone(),
                        namespace.clone(),
                        table_name.clone(),
                        plan_id,
                    )
                    .build()
                    .send()
                    .await
                    .unwrap();

                let fetch_result = fetch_resp.result().unwrap();
                assert!(
                    fetch_result.status == PlanningStatus::Completed
                        || fetch_result.status == PlanningStatus::Submitted,
                    "Should return valid planning status"
                );
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test fetching scan tasks for a completed plan
#[minio_macros::test(no_bucket)]
async fn fetch_scan_tasks(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Submit a scan plan
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();

            // If planning completed and returned plan tasks, try to fetch scan tasks
            if result.status == PlanningStatus::Completed && !result.plan_tasks.is_empty() {
                // Fetch scan tasks for the first plan task
                let fetch_resp = tables
                    .fetch_scan_tasks(
                        warehouse_name.clone(),
                        namespace.clone(),
                        table_name.clone(),
                        result.plan_tasks[0].clone(),
                    )
                    .build()
                    .send()
                    .await;

                // May succeed or fail depending on server state
                // Success means we got scan tasks back
                if let Ok(resp) = fetch_resp {
                    let _ = resp.result();
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test cancelling a planning operation
#[minio_macros::test(no_bucket)]
async fn cancel_planning(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Submit a scan plan
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();

            // If a plan_id was returned (async planning), try to cancel it
            if let Some(plan_id) = result.plan_id {
                let cancel_resp = tables
                    .cancel_planning(
                        warehouse_name.clone(),
                        namespace.clone(),
                        table_name.clone(),
                        plan_id,
                    )
                    .build()
                    .send()
                    .await;

                // Cancel might succeed or fail if planning already completed
                // Both are acceptable outcomes
                match cancel_resp {
                    Ok(resp) => {
                        assert!(resp.is_cancelled(), "Cancel should succeed");
                    }
                    Err(_) => {
                        // Planning may have already completed, which is fine
                    }
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test submitting a scan plan with filter expression
#[minio_macros::test(no_bucket)]
async fn plan_table_scan_with_filter(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Create a filter: id > 100
    let filter = FilterBuilder::column("id").gt(100);
    let filter_json: serde_json::Value = filter.to_json();

    // Submit a scan plan with filter
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .filter(filter_json)
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();
            assert!(
                result.status == PlanningStatus::Completed
                    || result.status == PlanningStatus::Submitted,
                "Should return completed or submitted status"
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test submitting a scan plan with complex AND filter
#[minio_macros::test(no_bucket)]
async fn plan_table_scan_with_and_filter(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Create a filter: id > 100 AND id < 1000
    let filter = FilterBuilder::column("id")
        .gt(100)
        .and(FilterBuilder::column("id").lt(1000));
    let filter_json: serde_json::Value = filter.to_json();

    // Verify the filter JSON structure
    assert_eq!(
        filter_json.get("type").and_then(|v| v.as_str()),
        Some("and"),
        "Filter should be an AND expression"
    );

    // Submit a scan plan with filter
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .filter(filter_json)
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();
            assert!(
                result.status == PlanningStatus::Completed
                    || result.status == PlanningStatus::Submitted,
                "Should return completed or submitted status"
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test submitting a scan plan with OR filter
#[minio_macros::test(no_bucket)]
async fn plan_table_scan_with_or_filter(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Create a filter: id = 1 OR id = 2
    let filter = FilterBuilder::column("id")
        .eq(1)
        .or(FilterBuilder::column("id").eq(2));
    let filter_json: serde_json::Value = filter.to_json();

    // Verify the filter JSON structure
    assert_eq!(
        filter_json.get("type").and_then(|v| v.as_str()),
        Some("or"),
        "Filter should be an OR expression"
    );

    // Submit a scan plan with filter
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .filter(filter_json)
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();
            assert!(
                result.status == PlanningStatus::Completed
                    || result.status == PlanningStatus::Submitted,
                "Should return completed or submitted status"
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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

/// Test submitting a scan plan with IS NULL filter
#[minio_macros::test(no_bucket)]
async fn plan_table_scan_with_null_filter(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Create a filter: data IS NULL
    let filter = FilterBuilder::column("data").is_null();
    let filter_json: serde_json::Value = filter.to_json();

    // Submit a scan plan with filter
    let resp: Result<PlanTableScanResponse, Error> = tables
        .plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .filter(filter_json)
        .build()
        .send()
        .await;

    // Check if scan planning is supported
    match resp {
        Ok(resp) => {
            let result = resp.result().unwrap();
            assert!(
                result.status == PlanningStatus::Completed
                    || result.status == PlanningStatus::Submitted,
                "Should return completed or submitted status"
            );
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Scan planning not supported by server, skipping test");
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
