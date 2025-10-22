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
async fn table_list_empty(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();

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

    // List tables in empty namespace
    let resp = tables
        .list_tables(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    assert!(resp.identifiers.is_empty());

    // Cleanup
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
