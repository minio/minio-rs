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
async fn table_rename(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();
    let new_table_name = rand_table_name();

    // Setup: Create warehouse, namespace, and table
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

    let schema = create_test_schema();
    tables
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

    // Rename table
    tables
        .rename_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            vec![namespace_name.clone()],
            &new_table_name,
        )
        .build()
        .send()
        .await
        .unwrap();

    // Verify old table name no longer exists
    let resp = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err());

    // Verify new table name exists
    let resp = tables
        .load_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &new_table_name,
        )
        .build()
        .send()
        .await;
    assert!(resp.is_ok());

    // Cleanup
    tables
        .delete_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &new_table_name,
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
