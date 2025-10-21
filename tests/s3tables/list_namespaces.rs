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
use minio::s3tables::response::ListNamespacesResponse;
use minio::s3tables::{HasPagination, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn namespace_list_empty(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // List namespaces in empty warehouse
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    // Verify pagination token
    let token = resp.next_token().unwrap();
    assert!(token.is_none());

    delete_warehouse_helper(warehouse_name, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn namespace_list_with_items(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let ns_name1 = rand_namespace_name();
    let ns_name2 = rand_namespace_name();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &ns_name1, &tables).await;
    create_namespace_helper(&warehouse_name, &ns_name2, &tables).await;

    // List namespaces and verify all properties
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify response content
    let namespaces = resp.namespaces().unwrap();
    assert_eq!(namespaces.len(), 2);
    assert!(namespaces.contains(&vec![ns_name1.clone()]));
    assert!(namespaces.contains(&vec![ns_name2.clone()]));

    // Verify pagination token
    let _ = resp.next_token().unwrap();

    delete_namespace_helper(&warehouse_name, &ns_name1, &tables).await;
    delete_namespace_helper(&warehouse_name, &ns_name2, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
