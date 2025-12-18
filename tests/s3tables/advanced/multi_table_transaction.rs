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
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1_name = rand_table_name();
    let table2_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    let schema = create_test_schema();
    let create_resp1: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table1_name.clone(),
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let original_metadata1 = create_resp1
        .table_result()
        .unwrap()
        .metadata_location
        .unwrap();

    let create_resp2: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table2_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    let original_metadata2 = create_resp2
        .table_result()
        .unwrap()
        .metadata_location
        .unwrap();

    // Use advanced Tier 2 API to atomically commit changes to both tables
    // This demonstrates capability not available in Tier 1 API
    let _transaction_resp = CommitMultiTableTransaction::builder()
        .client(tables.clone())
        .warehouse_name(warehouse_name.clone())
        .table_changes(vec![
            TableChange {
                identifier: TableIdentifier {
                    namespace: namespace.clone(),
                    name: table1_name.clone(),
                },
                requirements: vec![TableRequirement::AssertCreate],
                updates: vec![],
            },
            TableChange {
                identifier: TableIdentifier {
                    namespace: namespace.clone(),
                    name: table2_name.clone(),
                },
                requirements: vec![TableRequirement::AssertCreate],
                updates: vec![],
            },
        ])
        .build()
        .send()
        .await
        .unwrap();

    // Verify transaction succeeded by checking response is Ok (advanced response doesn't have warehouse() method)

    // Load both tables after transaction and verify they still exist
    let load_resp1_after: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table1_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_metadata1 = load_resp1_after
        .table_result()
        .unwrap()
        .metadata_location
        .unwrap();
    assert_eq!(loaded_metadata1, original_metadata1);

    let load_resp2_after: LoadTableResponse = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table2_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_metadata2 = load_resp2_after
        .table_result()
        .unwrap()
        .metadata_location
        .unwrap();
    assert_eq!(loaded_metadata2, original_metadata2);

    // Cleanup - delete both tables and verify they're gone
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table1_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(warehouse_name.clone(), namespace.clone(), table1_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table 1 should not exist after deletion");

    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table2_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(warehouse_name.clone(), namespace.clone(), table2_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table 2 should not exist after deletion");

    // Delete namespace and verify it's gone
    tables
        .delete_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .get_namespace(warehouse_name.clone(), namespace)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Namespace should not exist after deletion");

    // Delete warehouse and verify it's gone
    tables
        .delete_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables.get_warehouse(warehouse_name).build().send().await;
    assert!(resp.is_err(), "Warehouse should not exist after deletion");
}
