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
use minio::s3tables::response::ListWarehousesResponse;
use minio::s3tables::{HasPagination, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn warehouse_list(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // List warehouses and verify response properties
    let resp: ListWarehousesResponse = tables.list_warehouses().build().send().await.unwrap();
    // assert_eq!(resp.warehouse_name(), warehouse_name); TODO

    // Verify response content
    let warehouses: Vec<String> = resp.warehouses().unwrap();
    assert!(!warehouses.is_empty());
    println!("Warehouses: {:?}", warehouses);
    //assert!(warehouses.contains(&warehouse_name)); TODO

    // Verify pagination token method works (token may or may not exist)
    let _next_token = resp.next_token().unwrap();

    delete_warehouse_helper(warehouse_name, &tables).await;
}
