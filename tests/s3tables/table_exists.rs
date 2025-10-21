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
use minio::s3tables::response::{
    CreateTableResponse, DeleteTableResponse, LoadTableResponse, TableExistsResponse,
};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn table_exists_check(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    // Try to check if table exists (should fail - not created yet)
    let resp: Result<TableExistsResponse, Error> = tables
        .table_exists(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist before creation");

    // Create the table
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    let result = resp.table_result().unwrap();
    assert!(result.metadata_location.is_some());

    // Now check if table exists (should succeed)
    let resp = tables
        .table_exists(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Table should exist after creation");

    // Delete table and verify it no longer exists
    let _resp: DeleteTableResponse = tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<LoadTableResponse, Error> = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    // Try to check if deleted table exists (should fail)
    let resp: Result<_, Error> = tables
        .table_exists(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
