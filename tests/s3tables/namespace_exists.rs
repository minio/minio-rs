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
use minio::s3tables::response::NamespaceExistsResponse;
use minio::s3tables::{HasTablesFields, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn namespace_exists_check(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let namespace_vec: Vec<String> = vec![namespace_name.clone()];

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    // Now check if namespace exists (should succeed)
    let resp: NamespaceExistsResponse = tables
        .namespace_exists(&warehouse_name, namespace_vec.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.body().is_empty());

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
