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

//! Table listing tests inspired by MinIO server test suite.
//!
//! Test cases from MinIO server `tables-integration_test.go`:
//! - List empty tables
//! - List with items
//! - Pagination with page size and token

use super::common::*;
use minio::s3tables::response::ListTablesResponse;
use minio::s3tables::utils::{PageSize, TableName};
use minio::s3tables::{HasPagination, TablesApi};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn table_list_empty(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // List tables in empty namespace and verify all properties
    let resp: ListTablesResponse = tables
        .list_tables(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    // Note: ListTables response does not include warehouse name

    // Verify response is empty
    let identifiers = resp.identifiers().unwrap();
    assert!(identifiers.is_empty());

    // Verify pagination token
    let token = resp.next_token().unwrap();
    assert!(token.is_none());

    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn table_list_non_empty(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table1_name = rand_table_name();
    let table2_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table1_name.clone(),
        &tables,
    )
    .await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table2_name.clone(),
        &tables,
    )
    .await;

    let resp: ListTablesResponse = tables
        .list_tables(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    let identifiers = resp.identifiers().unwrap();
    assert_eq!(identifiers.len(), 2);

    let table_names: Vec<&str> = identifiers.iter().map(|id| id.name.as_str()).collect();
    assert!(table_names.contains(&table1_name.as_str()));
    assert!(table_names.contains(&table2_name.as_str()));

    for id in identifiers {
        assert_eq!(id.namespace_schema, vec![namespace.first().to_string()]);
    }

    let token = resp.next_token().unwrap();
    assert!(token.is_none());

    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table1_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_table(
            warehouse_name.clone(),
            namespace.clone(),
            table2_name.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test table pagination with multiple pages.
/// Corresponds to MinIO server test: "TestTablesIntegrationPagination" - table pagination
#[minio_macros::test(no_bucket)]
async fn table_list_pagination(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create multiple tables (8 to test pagination)
    let mut table_names: Vec<TableName> = Vec::new();
    let schema = create_test_schema();
    for i in 0..8 {
        let name = format!(
            "table_{:02}_{}",
            i,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        let table_name = TableName::try_from(name.as_str()).unwrap();
        tables
            .create_table(
                warehouse_name.clone(),
                namespace.clone(),
                table_name.clone(),
                schema.clone(),
            )
            .build()
            .send()
            .await
            .unwrap();
        table_names.push(table_name);
    }

    // List with small page size to force pagination
    let page_size: PageSize = PageSize::new(2).unwrap();
    let mut all_tables: Vec<String> = Vec::new();
    let mut page_token: Option<minio::s3tables::types::ContinuationToken> = None;
    let mut page_count = 0;

    loop {
        let resp: ListTablesResponse = match &page_token {
            Some(token) => tables
                .list_tables(warehouse_name.clone(), namespace.clone())
                .page_size(page_size)
                .page_token(token)
                .build()
                .send()
                .await
                .unwrap(),
            None => tables
                .list_tables(warehouse_name.clone(), namespace.clone())
                .page_size(page_size)
                .build()
                .send()
                .await
                .unwrap(),
        };

        page_count += 1;
        for id in resp.identifiers().unwrap() {
            all_tables.push(id.name.clone());
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

    // Verify we got all tables back
    assert_eq!(
        all_tables.len(),
        table_names.len(),
        "Expected {} tables, got {}",
        table_names.len(),
        all_tables.len()
    );

    // Verify pagination actually happened (should have more than 1 page with page_size=2 and 8 items)
    assert!(
        page_count > 1,
        "Expected multiple pages with page_size={} and {} items, got {} pages",
        page_size,
        table_names.len(),
        page_count
    );

    // Cleanup
    for table_name in &table_names {
        tables
            .delete_table(
                warehouse_name.clone(),
                namespace.clone(),
                table_name.clone(),
            )
            .build()
            .send()
            .await
            .unwrap();
    }
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
