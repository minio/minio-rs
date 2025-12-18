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

//! Namespace listing tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-integration_test.go`:
//! - List empty namespace
//! - List with items
//! - Pagination with page size and token

use super::common::*;
use minio::s3tables::response::ListNamespacesResponse;
use minio::s3tables::utils::{Namespace, PageSize};
use minio::s3tables::{HasNamespace, HasPagination, TablesApi};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn namespace_list_empty(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // List namespaces in empty warehouse
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    // Verify pagination token
    let token = resp.next_token().unwrap();
    assert!(token.is_none());

    delete_warehouse_helper(&warehouse, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn namespace_list_with_items(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let ns1 = rand_namespace();
    let ns2 = rand_namespace();

    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &ns1, &tables).await;
    create_namespace_helper(&warehouse, &ns2, &tables).await;

    // List namespaces and verify all properties
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify response content
    let namespaces = resp.namespaces().unwrap();
    assert_eq!(namespaces.len(), 2);
    assert!(namespaces.contains(&ns1));
    assert!(namespaces.contains(&ns2));

    // Verify pagination token
    let _ = resp.next_token().unwrap();

    delete_namespace_helper(&warehouse, &ns1, &tables).await;
    delete_namespace_helper(&warehouse, &ns2, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test namespace pagination with multiple pages.
/// Corresponds to MinIO server test: "TestTablesIntegrationPagination" - namespace pagination
#[minio_macros::test(no_bucket)]
async fn namespace_list_pagination(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Create multiple namespaces (10 to test pagination)
    let mut namespaces: Vec<Namespace> = Vec::new();
    for i in 0..10 {
        let ns_name = format!(
            "ns_{:02}_{}",
            i,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        let namespace = Namespace::try_from(vec![ns_name]).unwrap();
        tables
            .create_namespace(&warehouse, &namespace)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        namespaces.push(namespace);
    }

    // List with small page size to force pagination
    let page_size: PageSize = PageSize::new(3).unwrap();
    let mut all_namespaces: Vec<String> = Vec::new();
    let mut page_token: Option<minio::s3tables::types::ContinuationToken> = None;
    let mut page_count = 0;

    loop {
        let resp: ListNamespacesResponse = match &page_token {
            Some(token) => tables
                .list_namespaces(&warehouse)
                .unwrap()
                .page_size(page_size)
                .page_token(token)
                .build()
                .send()
                .await
                .unwrap(),
            None => tables
                .list_namespaces(&warehouse)
                .unwrap()
                .page_size(page_size)
                .build()
                .send()
                .await
                .unwrap(),
        };

        page_count += 1;
        for ns in resp.namespaces().unwrap() {
            if !ns.as_slice().is_empty() {
                all_namespaces.push(ns.first().to_string());
            }
        }

        match resp.next_token().unwrap() {
            Some(token) if !token.is_empty() => page_token = Some(token),
            _ => break,
        }

        // Safety check to prevent infinite loop
        if page_count > 10 {
            panic!("Too many pages returned, possible infinite loop");
        }
    }

    // Verify we got all namespaces back
    assert_eq!(
        all_namespaces.len(),
        namespaces.len(),
        "Expected {} namespaces, got {}",
        namespaces.len(),
        all_namespaces.len()
    );

    // Verify pagination actually happened (should have more than 1 page with page_size=3 and 10 items)
    assert!(
        page_count > 1,
        "Expected multiple pages with page_size={} and {} items, got {} pages",
        page_size,
        namespaces.len(),
        page_count
    );

    // Cleanup
    for namespace in namespaces {
        delete_namespace_helper(&warehouse, &namespace, &tables).await;
    }
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test listing namespaces with parent filter for multi-level namespaces.
/// Note: MinIO may not fully support multi-level namespaces as AWS S3 Tables does.
#[minio_macros::test(no_bucket)]
async fn namespace_list_with_parent_filter(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    create_warehouse_helper(&warehouse, &tables).await;

    // Create a parent namespace
    let parent_ns = rand_namespace();
    create_namespace_helper(&warehouse, &parent_ns, &tables).await;

    // Create child namespaces under the parent
    let child1_name = format!(
        "child1_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    let child2_name = format!(
        "child2_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    let child1_ns =
        Namespace::try_from(vec![parent_ns.first().to_string(), child1_name.clone()]).unwrap();
    let child2_ns =
        Namespace::try_from(vec![parent_ns.first().to_string(), child2_name.clone()]).unwrap();

    // Create child1 and check if server supports multi-level namespaces
    let child1_result = tables
        .create_namespace(&warehouse, child1_ns.clone())
        .unwrap()
        .build()
        .send()
        .await;

    let child1_resp = match child1_result {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!(
                "> Server failed to create child namespace: {:?}. Skipping parent filter test.",
                e
            );
            delete_namespace_helper(&warehouse, &parent_ns, &tables).await;
            delete_warehouse_helper(&warehouse, &tables).await;
            return;
        }
    };

    let returned_child1 = child1_resp.namespace_parts().unwrap();
    if returned_child1 != child1_ns.as_slice() {
        // Server doesn't support multi-level namespaces
        eprintln!(
            "> Server returns flattened namespace (got {:?}, expected {:?}). Skipping parent filter test.",
            returned_child1,
            child1_ns.as_slice()
        );
        // Clean up the flattened namespace
        let actual_ns = Namespace::try_from(returned_child1.to_vec()).unwrap();
        tables
            .delete_namespace(&warehouse, actual_ns)
            .unwrap()
            .build()
            .send()
            .await
            .ok();
        delete_namespace_helper(&warehouse, &parent_ns, &tables).await;
        delete_warehouse_helper(&warehouse, &tables).await;
        return;
    }

    tables
        .create_namespace(&warehouse, child2_ns.clone())
        .unwrap()
        .build()
        .send()
        .await
        .expect("Should create child2 namespace");

    // Create an unrelated top-level namespace
    let unrelated_ns = rand_namespace();
    create_namespace_helper(&warehouse, &unrelated_ns, &tables).await;

    // List namespaces with parent filter - should only return children
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse)
        .unwrap()
        .parent(parent_ns.clone())
        .build()
        .send()
        .await
        .expect("Should list namespaces with parent filter");

    let namespaces = resp.namespaces().unwrap();
    assert_eq!(
        namespaces.len(),
        2,
        "Should return exactly 2 child namespaces, got: {:?}",
        namespaces
    );

    // Verify the returned namespaces are the children
    assert!(
        namespaces.contains(&child1_ns),
        "Should contain child1: {:?} not in {:?}",
        child1_ns,
        namespaces
    );
    assert!(
        namespaces.contains(&child2_ns),
        "Should contain child2: {:?} not in {:?}",
        child2_ns,
        namespaces
    );

    // Cleanup - delete children first, then parent, then unrelated
    tables
        .delete_namespace(&warehouse, child1_ns)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Should delete child1");
    tables
        .delete_namespace(&warehouse, child2_ns)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Should delete child2");
    delete_namespace_helper(&warehouse, &parent_ns, &tables).await;
    delete_namespace_helper(&warehouse, &unrelated_ns, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}
