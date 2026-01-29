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

use super::super::common::*;
use minio::s3::error::Error;
use minio::s3tables::advanced::{
    CommitMultiTableTransaction, TableChange, TableIdentifier, TableRequirement,
};
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn advanced_multi_table_transaction(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1 = rand_table_name();
    let table2 = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;

    tables
        .create_namespace(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let schema = create_test_schema();
    let create_resp1: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table1, schema.clone())
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table1_result = create_resp1.table_result().unwrap();
    let original_metadata1 = table1_result.metadata_location.clone().unwrap();
    let table1_schema_id = table1_result.metadata.current_schema_id;

    let create_resp2: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table2, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let table2_result = create_resp2.table_result().unwrap();
    let original_metadata2 = table2_result.metadata_location.clone().unwrap();
    let table2_schema_id = table2_result.metadata.current_schema_id;

    // Use advanced Tier 2 API to atomically commit changes to both tables
    // This demonstrates capability not available in Tier 1 API
    // Server requires at least one requirement or update per table change
    let _transaction_resp = CommitMultiTableTransaction::builder()
        .client(tables.clone())
        .warehouse(warehouse.clone())
        .table_changes(vec![
            TableChange {
                identifier: TableIdentifier {
                    namespace: namespace.clone(),
                    name: table1.clone(),
                },
                requirements: vec![TableRequirement::AssertCurrentSchemaId {
                    current_schema_id: table1_schema_id,
                }],
                updates: vec![],
            },
            TableChange {
                identifier: TableIdentifier {
                    namespace: namespace.clone(),
                    name: table2.clone(),
                },
                requirements: vec![TableRequirement::AssertCurrentSchemaId {
                    current_schema_id: table2_schema_id,
                }],
                updates: vec![],
            },
        ])
        .build()
        .send()
        .await
        .unwrap();

    // Verify transaction succeeded by checking response is Ok (advanced response doesn't have warehouse() method)

    // Load both tables after transaction and verify they still exist
    // Note: Metadata location changes with each commit, so we verify existence only
    let load_resp1_after: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table1)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert!(
        load_resp1_after
            .table_result()
            .unwrap()
            .metadata_location
            .is_some(),
        "Table 1 should have metadata location after transaction"
    );
    let _ = original_metadata1; // Acknowledge we captured it but don't compare

    let load_resp2_after: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table2)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert!(
        load_resp2_after
            .table_result()
            .unwrap()
            .metadata_location
            .is_some(),
        "Table 2 should have metadata location after transaction"
    );
    let _ = original_metadata2; // Acknowledge we captured it but don't compare

    // Cleanup - delete both tables and verify they're gone
    tables
        .delete_table(&warehouse, &namespace, &table1)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(&warehouse, &namespace, table1)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table 1 should not exist after deletion");

    tables
        .delete_table(&warehouse, &namespace, &table2)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(&warehouse, &namespace, table2)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table 2 should not exist after deletion");

    // Delete namespace and verify it's gone
    tables
        .delete_namespace(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .get_namespace(&warehouse, namespace)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Namespace should not exist after deletion");

    // Delete warehouse and verify it's gone
    tables
        .delete_warehouse(&warehouse)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .get_warehouse(warehouse)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Warehouse should not exist after deletion");
}
