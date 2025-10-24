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
use minio::s3::tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn table_register(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let registered_table_name = rand_table_name();

    // Setup: Create warehouse and namespace
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    // Create initial table to get metadata location
    let schema = create_test_schema();
    let create_resp = tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    let metadata_location = create_resp.0.metadata_location.unwrap();

    // Register the table with a different name using the same metadata location
    let register_resp = tables
        .register_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &registered_table_name,
            &metadata_location,
        )
        .build()
        .send()
        .await
        .unwrap();

    assert!(register_resp.0.metadata_location.is_some());
    assert_eq!(
        register_resp.0.metadata_location.as_ref().unwrap(),
        &metadata_location
    );

    // Verify registered table exists and has correct metadata
    let load_resp = tables
        .load_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &registered_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(
        load_resp.0.metadata_location.as_ref().unwrap(),
        &metadata_location
    );

    // Cleanup
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &registered_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
}
