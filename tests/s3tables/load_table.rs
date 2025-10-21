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
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn table_load(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let namespace_vec = vec![namespace_name.clone()];

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    let schema = create_test_schema();
    let resp1: CreateTableResponse = tables
        .create_table(&warehouse_name, namespace_vec.clone(), &table_name, schema)
        .build()
        .send()
        .await
        .unwrap();

    let result = resp1.table_result().unwrap();
    let location: String = result.metadata_location.unwrap();

    // Load table and verify all properties
    let resp: LoadTableResponse = tables
        .load_table(&warehouse_name, namespace_vec.clone(), &table_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify response content matches creation response
    let table_results = resp.table_result().unwrap();
    assert_eq!(table_results.metadata_location.unwrap(), location);

    // Cleanup - delete table and verify it's gone
    tables
        .delete_table(&warehouse_name, namespace_vec.clone(), &table_name)
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(&warehouse_name, namespace_vec.clone(), &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
