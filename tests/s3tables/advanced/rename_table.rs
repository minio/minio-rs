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
use minio::s3tables::advanced::RenameTable;
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[allow(dead_code)]
//#[minio_macros::test(no_bucket)]
async fn advanced_rename_table_with_namespace_change(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let source_namespace_name = rand_namespace_name();
    let dest_namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let new_table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    tables
        .create_namespace(&warehouse_name, vec![source_namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    tables
        .create_namespace(&warehouse_name, vec![dest_namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    // Create table in source namespace
    let schema = create_test_schema();
    let create_resp: CreateTableResponse = tables
        .create_table(
            &warehouse_name,
            vec![source_namespace_name.clone()],
            &table_name,
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Verify table was created
    let original_metadata: String = create_resp
        .table_result()
        .unwrap()
        .metadata_location
        .unwrap();

    // Use advanced Tier 2 API to rename table and move to different namespace
    // This demonstrates capability not available in Tier 1 API
    let _rename_resp = RenameTable::builder()
        .client(tables.clone())
        .warehouse_name(&warehouse_name)
        .source_namespace(vec![source_namespace_name.clone()])
        .source_table_name(&table_name)
        .dest_namespace(vec![dest_namespace_name.clone()])
        .dest_table_name(&new_table_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify rename succeeded by checking response is Ok (advanced response doesn't have table() method)

    // Verify old table name no longer exists in source namespace
    let resp: Result<_, Error> = tables
        .load_table(
            &warehouse_name,
            vec![source_namespace_name.clone()],
            &table_name,
        )
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Old table should not exist in source namespace"
    );

    // Verify new table exists in destination namespace with preserved metadata
    let load_resp: LoadTableResponse = tables
        .load_table(
            &warehouse_name,
            vec![dest_namespace_name.clone()],
            &new_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();

    let loaded_result = load_resp.table_result().unwrap();
    assert_eq!(loaded_result.metadata_location.unwrap(), original_metadata);

    // Cleanup - delete table from destination namespace
    tables
        .delete_table(
            &warehouse_name,
            vec![dest_namespace_name.clone()],
            &new_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(
            &warehouse_name,
            vec![dest_namespace_name.clone()],
            &new_table_name,
        )
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    // Delete both namespaces and verify they're gone
    tables
        .delete_namespace(&warehouse_name, vec![source_namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .get_namespace(&warehouse_name, vec![source_namespace_name])
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Source namespace should not exist after deletion"
    );

    tables
        .delete_namespace(&warehouse_name, vec![dest_namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .get_namespace(&warehouse_name, vec![dest_namespace_name])
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Destination namespace should not exist after deletion"
    );

    // Delete warehouse and verify it's gone
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables.get_warehouse(&warehouse_name).build().send().await;
    assert!(resp.is_err(), "Warehouse should not exist after deletion");
}
