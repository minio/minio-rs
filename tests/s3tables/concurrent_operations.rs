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

//! Concurrent operations tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-api-handlers_test.go`:
//! - Concurrent warehouse creation (only one succeeds)
//! - Concurrent table creation
//! - Verify proper conflict handling

use super::common::*;
use futures_util::future::join_all;
use minio::s3tables::TablesApi;
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio_common::test_context::TestContext;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Test concurrent warehouse creation - only one should succeed.
/// Corresponds to MinIO server test: "TestTablesCreateWarehouseAPIConcurrent"
#[minio_macros::test(no_bucket)]
async fn concurrent_warehouse_creation(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name_str = format!(
        "concurrent-warehouse-{}",
        &uuid::Uuid::new_v4().to_string()[..8]
    );
    let warehouse_name = WarehouseName::try_from(warehouse_name_str.as_str()).unwrap();

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch 5 concurrent create requests
    let num_requests = 5;
    let mut handles = Vec::new();

    for _ in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse_name.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let result = tables_clone
                .create_warehouse(warehouse_clone)
                .build()
                .send()
                .await;

            match result {
                Ok(_) => {
                    success_counter.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    conflict_counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    // Exactly one should succeed, rest should fail with conflict
    assert_eq!(
        successes, 1,
        "Exactly one concurrent warehouse creation should succeed"
    );
    assert_eq!(
        conflicts,
        num_requests - 1,
        "All other requests should fail with conflict"
    );

    // Cleanup
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test concurrent namespace creation - only one should succeed.
/// Similar to warehouse concurrency test
#[minio_macros::test(no_bucket)]
async fn concurrent_namespace_creation(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace_name = format!(
        "concurrent_ns_{}",
        &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
    );
    let namespace = Namespace::try_from(vec![namespace_name]).unwrap();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch 5 concurrent create requests
    let num_requests = 5;
    let mut handles = Vec::new();

    for _ in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse_name.clone();
        let namespace_clone = namespace.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let result = tables_clone
                .create_namespace(warehouse_clone, namespace_clone)
                .build()
                .send()
                .await;

            match result {
                Ok(_) => {
                    success_counter.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    conflict_counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    // Exactly one should succeed, rest should fail with conflict
    assert_eq!(
        successes, 1,
        "Exactly one concurrent namespace creation should succeed"
    );
    assert_eq!(
        conflicts,
        num_requests - 1,
        "All other requests should fail with conflict"
    );

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test concurrent table creation - only one should succeed.
/// Similar to warehouse concurrency test
#[minio_macros::test(no_bucket)]
async fn concurrent_table_creation(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name_str = format!(
        "concurrent_table_{}",
        &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
    );
    let table_name = TableName::try_from(table_name_str.as_str()).unwrap();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch 5 concurrent create requests
    let num_requests = 5;
    let mut handles = Vec::new();

    for _ in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse_name.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table_name.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let schema = create_test_schema();
            let result = tables_clone
                .create_table(warehouse_clone, namespace_clone, table_clone, schema)
                .build()
                .send()
                .await;

            match result {
                Ok(_) => {
                    success_counter.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    conflict_counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    // Exactly one should succeed, rest should fail with conflict
    assert_eq!(
        successes, 1,
        "Exactly one concurrent table creation should succeed"
    );
    assert_eq!(
        conflicts,
        num_requests - 1,
        "All other requests should fail with conflict"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .ok();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test multiple different tables can be created concurrently.
/// Different tables should all succeed
#[minio_macros::test(no_bucket)]
async fn concurrent_different_table_creation(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let success_count = Arc::new(AtomicUsize::new(0));

    // Launch 5 concurrent create requests for DIFFERENT tables
    let num_requests = 5;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse_name.clone();
        let namespace_clone = namespace.clone();
        let table_name_str = format!(
            "diff_table_{}_{}",
            i,
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
        );
        let success_counter = Arc::clone(&success_count);

        let handle = tokio::spawn(async move {
            let schema = create_test_schema();
            let table_name = TableName::try_from(table_name_str.as_str()).unwrap();
            let result = tables_clone
                .create_table(warehouse_clone, namespace_clone, table_name.clone(), schema)
                .build()
                .send()
                .await;

            if result.is_ok() {
                success_counter.fetch_add(1, Ordering::SeqCst);
            }
            (table_name, result.is_ok())
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    let results: Vec<_> = join_all(handles).await;
    let successes = success_count.load(Ordering::SeqCst);

    // All should succeed since they're different tables
    assert_eq!(
        successes, num_requests,
        "All concurrent creations of different tables should succeed"
    );

    // Cleanup all created tables
    for result in results {
        if let Ok((table_name, created)) = result
            && created
        {
            tables
                .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
                .build()
                .send()
                .await
                .ok();
        }
    }
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
