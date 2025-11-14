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
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse, RegisterTableResponse};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn table_register(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let registered_table_name = rand_table_name();
    let namespace_vec = vec![namespace_name.clone()];

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    // Create initial table to get metadata location
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(&warehouse_name, namespace_vec.clone(), &table_name, schema)
        .build()
        .send()
        .await
        .unwrap();

    let table_result = resp.table_result().unwrap();
    let metadata_location: String = table_result.metadata_location.unwrap();
    assert!(metadata_location.starts_with(&format!("s3://{warehouse_name}/")));

    // Register the table with a different name using the same metadata location
    let register_resp: RegisterTableResponse = tables
        .register_table(
            &warehouse_name,
            namespace_vec.clone(),
            &registered_table_name,
            &metadata_location,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Verify register response metadata
    let register_result = register_resp.table_result().unwrap();
    assert_eq!(
        register_result.metadata_location.as_ref().unwrap(),
        &metadata_location
    );

    // Verify registered table exists and has correct metadata
    let load_resp: LoadTableResponse = tables
        .load_table(
            &warehouse_name,
            namespace_vec.clone(),
            &registered_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Verify load response
    let load_result = load_resp.table_result().unwrap();
    assert_eq!(
        load_result.metadata_location.as_ref().unwrap(),
        &metadata_location
    );

    // Cleanup - delete tables and verify they're gone
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

    tables
        .delete_table(
            &warehouse_name,
            namespace_vec.clone(),
            &registered_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();
    let resp: Result<_, Error> = tables
        .load_table(
            &warehouse_name,
            namespace_vec.clone(),
            &registered_table_name,
        )
        .build()
        .send()
        .await;
    assert!(
        resp.is_err(),
        "Registered table should not exist after deletion"
    );

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
