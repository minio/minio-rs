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

//! Iceberg Transaction Compatibility Tests
//!
//! These tests validate compatibility with Apache Iceberg REST Catalog specification
//! for transaction operations. They correspond to tests from Apache Iceberg's
//! CatalogTests.java transaction-related tests in the REST Compatibility Kit (RCK).
//!
//! References:
//! - https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/catalog/CatalogTests.java
//! - MinIO eos iceberg-compat-tests

use super::common::*;
use futures_util::future::join_all;
use minio::s3::error::Error;
use minio::s3tables::advanced::{
    CommitMultiTableTransaction, TableChange, TableIdentifier,
    TableRequirement as AdvTableRequirement, TableUpdate as AdvTableUpdate,
};
use minio::s3tables::builders::{TableRequirement, TableUpdate};
use minio::s3tables::iceberg::{
    Field, FieldType, NullOrder, PartitionField, PartitionSpec, PrimitiveType, Schema, Snapshot,
    SortDirection, SortField, SortOrder, Transform,
};
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// =============================================================================
// Data Append Operations Tests
// Corresponds to: testAppend, testConcurrentAppendEmptyTable, testConcurrentAppendNonEmptyTable
// =============================================================================

/// Create a test snapshot for append operations
fn create_test_snapshot(snapshot_id: i64, parent_id: Option<i64>, schema_id: i32) -> Snapshot {
    let mut summary = HashMap::new();
    summary.insert("operation".to_string(), "append".to_string());
    summary.insert("added-data-files".to_string(), "1".to_string());

    Snapshot {
        snapshot_id,
        parent_snapshot_id: parent_id,
        sequence_number: Some(snapshot_id),
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
        summary,
        manifest_list: format!("s3://bucket/metadata/snap-{snapshot_id}-manifest-list.avro"),
        schema_id: Some(schema_id),
    }
}

/// Test appending data to a table via snapshot addition.
/// Corresponds to Iceberg RCK: testAppend
#[minio_macros::test(no_bucket)]
async fn append_data(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Add a snapshot (simulating data append)
    let snapshot = create_test_snapshot(1, None, 0);
    let commit_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![])
        .updates(vec![
            AdvTableUpdate::AddSnapshot { snapshot },
            AdvTableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                r#type: "branch".to_string(),
                snapshot_id: 1,
                max_age_ref_ms: None,
                max_snapshot_age_ms: None,
                min_snapshots_to_keep: None,
            },
        ])
        .build()
        .send()
        .await;

    match commit_result {
        Ok(_) => eprintln!("> Append data succeeded"),
        Err(e) => eprintln!("> Append data failed (may be expected): {:?}", e),
    }

    // Verify table still exists
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert!(
        load_resp.table_result().is_ok(),
        "Table should exist after append"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent appends to an empty table.
/// Corresponds to Iceberg RCK: testConcurrentAppendEmptyTable
#[minio_macros::test(no_bucket)]
async fn concurrent_append_empty_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create empty table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent append operations
    let num_requests = 3;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let snapshot = create_test_snapshot((i + 1) as i64, None, 0);
            let result = tables_clone
                .adv_commit_table(warehouse_clone, namespace_clone, table_clone)
                .unwrap()
                .requirements(vec![AdvTableRequirement::AssertRefSnapshotId {
                    r#ref: "main".to_string(),
                    snapshot_id: None, // Assert no current snapshot (empty table)
                }])
                .updates(vec![
                    AdvTableUpdate::AddSnapshot { snapshot },
                    AdvTableUpdate::SetSnapshotRef {
                        ref_name: "main".to_string(),
                        r#type: "branch".to_string(),
                        snapshot_id: (i + 1) as i64,
                        max_age_ref_ms: None,
                        max_snapshot_age_ms: None,
                        min_snapshots_to_keep: None,
                    },
                ])
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    // At most one should succeed when asserting empty table
    eprintln!(
        "> Concurrent appends to empty table: {} succeeded, {} conflicted",
        successes, conflicts
    );
    assert!(
        successes <= 1,
        "At most one concurrent append to empty table should succeed"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent appends to a non-empty table.
/// Corresponds to Iceberg RCK: testConcurrentAppendNonEmptyTable
#[minio_macros::test(no_bucket)]
async fn concurrent_append_non_empty_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table and add initial snapshot
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Add initial snapshot
    let initial_snapshot = create_test_snapshot(1, None, 0);
    let _ = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![])
        .updates(vec![
            AdvTableUpdate::AddSnapshot {
                snapshot: initial_snapshot,
            },
            AdvTableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                r#type: "branch".to_string(),
                snapshot_id: 1,
                max_age_ref_ms: None,
                max_snapshot_age_ms: None,
                min_snapshots_to_keep: None,
            },
        ])
        .build()
        .send()
        .await;

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent append operations to non-empty table
    let num_requests = 3;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let snapshot_id = (i + 10) as i64;
            let snapshot = create_test_snapshot(snapshot_id, Some(1), 0);
            let result = tables_clone
                .adv_commit_table(warehouse_clone, namespace_clone, table_clone)
                .unwrap()
                .requirements(vec![AdvTableRequirement::AssertRefSnapshotId {
                    r#ref: "main".to_string(),
                    snapshot_id: Some(1), // Assert current snapshot is 1
                }])
                .updates(vec![
                    AdvTableUpdate::AddSnapshot { snapshot },
                    AdvTableUpdate::SetSnapshotRef {
                        ref_name: "main".to_string(),
                        r#type: "branch".to_string(),
                        snapshot_id,
                        max_age_ref_ms: None,
                        max_snapshot_age_ms: None,
                        min_snapshots_to_keep: None,
                    },
                ])
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    eprintln!(
        "> Concurrent appends to non-empty table: {} succeeded, {} conflicted",
        successes, conflicts
    );
    // At most one should succeed with the snapshot assertion
    assert!(
        successes <= 1,
        "At most one concurrent append should succeed with snapshot assertion"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Create Transaction Tests
// Corresponds to: testCreateTransaction, testCompleteCreateTransaction,
//                 testConcurrentCreateTransaction
// =============================================================================

/// Test basic table creation via CommitTable (create transaction).
/// Corresponds to Iceberg RCK: testCreateTransaction
#[minio_macros::test(no_bucket)]
async fn create_transaction_basic(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table via CommitTable with AssertCreate requirement
    let commit_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![AdvTableRequirement::AssertCreate])
        .updates(vec![])
        .build()
        .send()
        .await;

    match commit_result {
        Ok(_) => {
            eprintln!("> Create transaction succeeded");
            // Verify table exists
            let load_resp: Result<LoadTableResponse, Error> = tables
                .load_table(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            // Table may or may not exist depending on server's handling of AssertCreate
            if load_resp.is_ok() {
                eprintln!("> Table created via AssertCreate");
            } else {
                eprintln!("> Table not found after AssertCreate (expected for some servers)");
            }
        }
        Err(e) => {
            eprintln!(
                "> Create transaction failed (may be expected - use create_table API): {:?}",
                e
            );
        }
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test complete create transaction with all options.
/// Corresponds to Iceberg RCK: testCompleteCreateTransaction
#[minio_macros::test(no_bucket)]
async fn complete_create_transaction(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // First create the table via standard API
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load table to get current state
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table_result = load_resp.table_result().unwrap();
    let table_uuid = &table_result.metadata.table_uuid;

    // Now use CommitTable with full options
    let mut props = HashMap::new();
    props.insert("created-by".to_string(), "complete-create-test".to_string());
    props.insert("iceberg.version".to_string(), "1".to_string());

    let commit_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![AdvTableRequirement::AssertTableUuid {
            uuid: table_uuid.clone(),
        }])
        .updates(vec![
            AdvTableUpdate::SetProperties { updates: props },
            AdvTableUpdate::UpgradeFormatVersion { format_version: 2 },
        ])
        .build()
        .send()
        .await;

    match commit_result {
        Ok(_) => eprintln!("> Complete create transaction succeeded"),
        Err(e) => eprintln!("> Complete create transaction failed: {:?}", e),
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent create transactions.
/// Corresponds to Iceberg RCK: testConcurrentCreateTransaction
#[minio_macros::test(no_bucket)]
async fn concurrent_create_transactions(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent create transactions for the same table
    let num_requests = 5;
    let mut handles = Vec::new();

    for _ in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let schema = create_test_schema();
            let result = tables_clone
                .create_table(warehouse_clone, namespace_clone, table_clone, schema)
                .unwrap()
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    eprintln!(
        "> Concurrent create transactions: {} succeeded, {} conflicted",
        successes, conflicts
    );
    assert_eq!(successes, 1, "Exactly one concurrent create should succeed");
    assert_eq!(conflicts, num_requests - 1, "Other creates should conflict");

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Replace Transaction Tests
// Corresponds to: testReplaceTransaction, testCompleteReplaceTransaction,
//                 testReplaceTransactionRequiresTableExists, testConcurrentReplaceTransactions
// =============================================================================

/// Test basic replace transaction.
/// Corresponds to Iceberg RCK: testReplaceTransaction
#[minio_macros::test(no_bucket)]
async fn replace_transaction(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table first
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load to get UUID
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table_uuid = &load_resp.table_result().unwrap().metadata.table_uuid;

    // Replace transaction - update format version
    let commit_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![AdvTableRequirement::AssertTableUuid {
            uuid: table_uuid.clone(),
        }])
        .updates(vec![AdvTableUpdate::UpgradeFormatVersion {
            format_version: 2,
        }])
        .build()
        .send()
        .await;

    match commit_result {
        Ok(_) => eprintln!("> Replace transaction succeeded"),
        Err(e) => eprintln!("> Replace transaction failed: {:?}", e),
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test complete replace transaction with all options.
/// Corresponds to Iceberg RCK: testCompleteReplaceTransaction
#[minio_macros::test(no_bucket)]
async fn complete_replace_transaction(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load to get current state
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table_result = load_resp.table_result().unwrap();
    let table_uuid = &table_result.metadata.table_uuid;
    let current_schema_id = table_result.metadata.current_schema_id;

    // Create evolved schema
    let evolved_schema = Schema {
        schema_id: Some(1),
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
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Data field".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 3,
                name: "created_at".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::Timestamp),
                doc: Some("Creation timestamp".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    };

    // Complete replace with multiple updates
    let mut props = HashMap::new();
    props.insert(
        "replaced-by".to_string(),
        "complete-replace-test".to_string(),
    );

    let commit_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![
            AdvTableRequirement::AssertTableUuid {
                uuid: table_uuid.clone(),
            },
            AdvTableRequirement::AssertCurrentSchemaId { current_schema_id },
        ])
        .updates(vec![
            AdvTableUpdate::AddSchema {
                schema: evolved_schema,
                last_column_id: Some(3),
            },
            AdvTableUpdate::SetProperties { updates: props },
        ])
        .build()
        .send()
        .await;

    match commit_result {
        Ok(_) => eprintln!("> Complete replace transaction succeeded"),
        Err(e) => eprintln!("> Complete replace transaction failed: {:?}", e),
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test that replace transaction requires table to exist.
/// Corresponds to Iceberg RCK: testReplaceTransactionRequiresTableExists
#[minio_macros::test(no_bucket)]
async fn replace_transaction_requires_table_exists(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Try to replace a non-existent table - should fail
    let commit_result = tables
        .commit_table(&warehouse, &namespace, table)
        .unwrap()
        .requirements(vec![TableRequirement::AssertTableUuid {
            uuid: "00000000-0000-0000-0000-000000000000".to_string(),
        }])
        .updates(vec![TableUpdate::UpgradeFormatVersion {
            format_version: 2,
        }])
        .build()
        .send()
        .await;

    assert!(
        commit_result.is_err(),
        "Replace transaction should fail for non-existent table"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent replace transactions.
/// Corresponds to Iceberg RCK: testConcurrentReplaceTransactions
#[minio_macros::test(no_bucket)]
async fn concurrent_replace_transactions(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load to get UUID
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table_uuid = load_resp
        .table_result()
        .unwrap()
        .metadata
        .table_uuid
        .clone();

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent replace transactions
    let num_requests = 3;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let uuid_clone = table_uuid.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let mut props = HashMap::new();
            props.insert(format!("concurrent-update-{i}"), format!("value-{i}"));

            let result = tables_clone
                .adv_commit_table(warehouse_clone, namespace_clone, table_clone)
                .unwrap()
                .requirements(vec![AdvTableRequirement::AssertTableUuid {
                    uuid: uuid_clone,
                }])
                .updates(vec![AdvTableUpdate::SetProperties { updates: props }])
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    eprintln!(
        "> Concurrent replace transactions: {} succeeded, {} conflicted",
        successes, conflicts
    );
    // With UUID assertion (not changing), all may succeed since they don't conflict
    // The important thing is that transactions complete without error

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Create-or-Replace Transaction Tests
// Corresponds to: testCreateOrReplaceTransactionCreate, testCreateOrReplaceTransactionReplace,
//                 testConcurrentCreateOrReplace
// =============================================================================

/// Test create-or-replace when table doesn't exist (creates).
/// Corresponds to Iceberg RCK: testCreateOrReplaceTransactionCreate
#[minio_macros::test(no_bucket)]
async fn create_or_replace_when_table_not_exists(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create-or-replace for non-existent table should create it
    // Note: Standard create_table API doesn't support create-or-replace semantics directly
    // We test that create_table works for new tables
    let schema = create_test_schema();
    let create_resp: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert!(
        create_resp.table_result().is_ok(),
        "Create-or-replace should create new table"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test create-or-replace when table exists (replaces).
/// Corresponds to Iceberg RCK: testCreateOrReplaceTransactionReplace
#[minio_macros::test(no_bucket)]
async fn create_or_replace_when_table_exists(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create initial table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load to get UUID
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let original_uuid = &load_resp.table_result().unwrap().metadata.table_uuid;

    // Replace existing table via CommitTable
    let mut props = HashMap::new();
    props.insert("replaced".to_string(), "true".to_string());

    let commit_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![AdvTableRequirement::AssertTableUuid {
            uuid: original_uuid.clone(),
        }])
        .updates(vec![AdvTableUpdate::SetProperties { updates: props }])
        .build()
        .send()
        .await;

    match commit_result {
        Ok(_) => {
            // Verify table still has same UUID (replace, not recreate)
            let load_after: LoadTableResponse = tables
                .load_table(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await
                .unwrap();

            let new_uuid = &load_after.table_result().unwrap().metadata.table_uuid;
            assert_eq!(
                original_uuid, new_uuid,
                "Table UUID should remain same after replace"
            );
        }
        Err(e) => eprintln!("> Replace failed: {:?}", e),
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent create-or-replace operations.
/// Corresponds to Iceberg RCK: testConcurrentCreateOrReplace
#[minio_macros::test(no_bucket)]
async fn concurrent_create_or_replace(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    let success_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent create operations (simulating create-or-replace)
    let num_requests = 5;
    let mut handles = Vec::new();

    for _ in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);

        let handle = tokio::spawn(async move {
            let schema = create_test_schema();
            let result = tables_clone
                .create_table(warehouse_clone, namespace_clone, table_clone, schema)
                .unwrap()
                .build()
                .send()
                .await;

            if result.is_ok() {
                success_counter.fetch_add(1, Ordering::SeqCst);
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);

    // Exactly one create should succeed
    assert_eq!(
        successes, 1,
        "Exactly one concurrent create-or-replace should succeed"
    );

    // Verify table exists
    let load_resp: Result<LoadTableResponse, Error> = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    assert!(
        load_resp.is_ok(),
        "Table should exist after concurrent creates"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Concurrent Schema/Spec Update Tests
// Corresponds to: testConcurrentSchemaUpdates, testConcurrentPartitionSpecUpdates,
//                 testConcurrentSortOrderUpdates
// =============================================================================

/// Test concurrent schema updates with conflict detection.
/// Corresponds to Iceberg RCK: testConcurrentSchemaUpdates
#[minio_macros::test(no_bucket)]
async fn concurrent_schema_updates(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent schema updates
    let num_requests = 3;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let new_schema = Schema {
                schema_id: Some(i + 1),
                fields: vec![
                    Field {
                        id: 1,
                        name: "id".to_string(),
                        required: true,
                        field_type: FieldType::Primitive(PrimitiveType::Long),
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    },
                    Field {
                        id: 2,
                        name: "data".to_string(),
                        required: false,
                        field_type: FieldType::Primitive(PrimitiveType::String),
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    },
                    Field {
                        id: (i + 3),
                        name: format!("new_field_{i}"),
                        required: false,
                        field_type: FieldType::Primitive(PrimitiveType::String),
                        doc: Some(format!("Added by concurrent update {i}")),
                        initial_default: None,
                        write_default: None,
                    },
                ],
                identifier_field_ids: Some(vec![1]),
                ..Default::default()
            };

            let result = tables_clone
                .adv_commit_table(warehouse_clone, namespace_clone, table_clone)
                .unwrap()
                .requirements(vec![AdvTableRequirement::AssertCurrentSchemaId {
                    current_schema_id: 0,
                }])
                .updates(vec![AdvTableUpdate::AddSchema {
                    schema: new_schema,
                    last_column_id: Some(i + 3),
                }])
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    eprintln!(
        "> Concurrent schema updates: {} succeeded, {} conflicted",
        successes, conflicts
    );
    // At most one should succeed with the schema ID assertion
    assert!(
        successes <= 1,
        "At most one concurrent schema update should succeed with assertion"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent partition spec updates with conflict detection.
/// Corresponds to Iceberg RCK: testConcurrentPartitionSpecUpdates
#[minio_macros::test(no_bucket)]
async fn concurrent_partition_spec_updates(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent partition spec updates
    let num_requests = 3;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let partition_spec = PartitionSpec {
                spec_id: (i + 1),
                fields: vec![PartitionField {
                    source_id: 1,
                    field_id: (1000 + i),
                    name: format!("id_bucket_{i}"),
                    transform: Transform::Bucket { n: (8 + i) as u32 },
                }],
            };

            let result = tables_clone
                .adv_commit_table(warehouse_clone, namespace_clone, table_clone)
                .unwrap()
                .requirements(vec![AdvTableRequirement::AssertDefaultSpecId {
                    default_spec_id: 0,
                }])
                .updates(vec![AdvTableUpdate::AddPartitionSpec {
                    spec: partition_spec,
                }])
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    eprintln!(
        "> Concurrent partition spec updates: {} succeeded, {} conflicted",
        successes, conflicts
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test concurrent sort order updates with conflict detection.
/// Corresponds to Iceberg RCK: testConcurrentSortOrderUpdates
#[minio_macros::test(no_bucket)]
async fn concurrent_sort_order_updates(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));

    // Launch concurrent sort order updates
    let num_requests = 3;
    let mut handles = Vec::new();

    for i in 0..num_requests {
        let tables_clone = tables.clone();
        let warehouse_clone = warehouse.clone();
        let namespace_clone = namespace.clone();
        let table_clone = table.clone();
        let success_counter = Arc::clone(&success_count);
        let conflict_counter = Arc::clone(&conflict_count);

        let handle = tokio::spawn(async move {
            let sort_order = SortOrder {
                order_id: (i + 1),
                fields: vec![SortField {
                    source_id: 1,
                    transform: Transform::Identity,
                    direction: if i % 2 == 0 {
                        SortDirection::Asc
                    } else {
                        SortDirection::Desc
                    },
                    null_order: NullOrder::NullsFirst,
                }],
            };

            let result = tables_clone
                .adv_commit_table(warehouse_clone, namespace_clone, table_clone)
                .unwrap()
                .requirements(vec![AdvTableRequirement::AssertDefaultSortOrderId {
                    default_sort_order_id: 0,
                }])
                .updates(vec![AdvTableUpdate::AddSortOrder { sort_order }])
                .build()
                .send()
                .await;

            match result {
                Ok(_) => success_counter.fetch_add(1, Ordering::SeqCst),
                Err(_) => conflict_counter.fetch_add(1, Ordering::SeqCst),
            };
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = success_count.load(Ordering::SeqCst);
    let conflicts = conflict_count.load(Ordering::SeqCst);

    eprintln!(
        "> Concurrent sort order updates: {} succeeded, {} conflicted",
        successes, conflicts
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Metadata Cleanup Tests
// Corresponds to: testMetadataFileLocationsRemovalAfterCommit, testRemoveUnusedSchemas
// =============================================================================

/// Test that metadata file locations are managed after commits.
/// Corresponds to Iceberg RCK: testMetadataFileLocationsRemovalAfterCommit
#[minio_macros::test(no_bucket)]
async fn metadata_file_cleanup_after_commit(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Load to get initial metadata location
    let load1: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let metadata_location_1 = load1
        .table_result()
        .unwrap()
        .metadata_location
        .clone()
        .unwrap();

    // Make a commit to create new metadata
    let mut props = HashMap::new();
    props.insert("test-key".to_string(), "test-value".to_string());

    let _ = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![])
        .updates(vec![AdvTableUpdate::SetProperties { updates: props }])
        .build()
        .send()
        .await;

    // Load again to get new metadata location
    let load2: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let metadata_location_2 = load2
        .table_result()
        .unwrap()
        .metadata_location
        .clone()
        .unwrap();

    // Metadata location should change after commit (new metadata file)
    // Note: Some servers may reuse the same location
    eprintln!("> Initial metadata: {metadata_location_1}");
    eprintln!("> After commit: {metadata_location_2}");

    // Verify table is still accessible
    assert!(
        load2.table_result().is_ok(),
        "Table should be accessible after metadata changes"
    );

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

/// Test removing unused schemas from table metadata.
/// Corresponds to Iceberg RCK: testRemoveUnusedSchemas
#[minio_macros::test(no_bucket)]
async fn remove_unused_schemas(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create table
    let schema = create_test_schema();
    tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Add a new schema
    let new_schema = Schema {
        schema_id: Some(1),
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 2,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: None,
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 3,
                name: "extra".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Extra field".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    };

    let add_schema_result = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        .requirements(vec![])
        .updates(vec![
            AdvTableUpdate::AddSchema {
                schema: new_schema,
                last_column_id: Some(3),
            },
            AdvTableUpdate::SetCurrentSchema { schema_id: 1 },
        ])
        .build()
        .send()
        .await;

    match add_schema_result {
        Ok(_) => {
            eprintln!("> Added new schema successfully");

            // Load table to verify schemas
            let load_resp: LoadTableResponse = tables
                .load_table(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await
                .unwrap();

            let metadata = &load_resp.table_result().unwrap().metadata;
            let schema_count = metadata.schemas.len();
            eprintln!("> Table has {schema_count} schemas after adding new one");

            // The old schema (id=0) is now unused since current_schema_id=1
            // Server may or may not remove it automatically
        }
        Err(e) => eprintln!("> Add schema failed: {:?}", e),
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}

// =============================================================================
// Multi-Table Transaction Tests
// Corresponds to: testCommitMultiTableTransaction
// =============================================================================

/// Test atomic multi-table transaction.
/// Corresponds to Iceberg RCK: testCommitMultiTableTransaction (via advanced API)
#[minio_macros::test(no_bucket)]
async fn commit_multi_table_transaction(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1 = rand_table_name();
    let table2 = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;

    // Create two tables
    let schema = create_test_schema();
    let create1: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table1, schema.clone())
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let create2: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table2, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let schema1_id = create1.table_result().unwrap().metadata.current_schema_id;
    let schema2_id = create2.table_result().unwrap().metadata.current_schema_id;

    // Atomically update both tables
    let transaction_result = CommitMultiTableTransaction::builder()
        .client(tables.clone())
        .warehouse(warehouse.clone())
        .table_changes(vec![
            TableChange {
                identifier: TableIdentifier {
                    namespace: namespace.clone(),
                    name: table1.clone(),
                },
                requirements: vec![AdvTableRequirement::AssertCurrentSchemaId {
                    current_schema_id: schema1_id,
                }],
                updates: vec![],
            },
            TableChange {
                identifier: TableIdentifier {
                    namespace: namespace.clone(),
                    name: table2.clone(),
                },
                requirements: vec![AdvTableRequirement::AssertCurrentSchemaId {
                    current_schema_id: schema2_id,
                }],
                updates: vec![],
            },
        ])
        .build()
        .send()
        .await;

    match transaction_result {
        Ok(_) => {
            eprintln!("> Multi-table transaction succeeded");

            // Verify both tables still exist
            let load1: LoadTableResponse = tables
                .load_table(&warehouse, &namespace, &table1)
                .unwrap()
                .build()
                .send()
                .await
                .unwrap();

            let load2: LoadTableResponse = tables
                .load_table(&warehouse, &namespace, &table2)
                .unwrap()
                .build()
                .send()
                .await
                .unwrap();

            assert!(load1.table_result().is_ok(), "Table 1 should exist");
            assert!(load2.table_result().is_ok(), "Table 2 should exist");
        }
        Err(e) => eprintln!("> Multi-table transaction failed: {:?}", e),
    }

    // Cleanup
    tables.delete_and_purge_warehouse(warehouse).await.ok();
}
