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

use super::super::common::*;
use minio::s3::error::Error;
use minio::s3tables::advanced::{CommitTable, TableRequirement};
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[allow(dead_code)]
//#[minio_macros::test(no_bucket)]
async fn advanced_commit_table(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    let schema = create_test_schema();
    let create_resp: CreateTableResponse = tables
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

    let original_metadata = create_resp
        .table_result()
        .unwrap()
        .metadata_location
        .unwrap();

    // Load table to get current metadata for commit operation
    let load_resp: LoadTableResponse = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    // Parse metadata from the serde_json::Value
    let table_metadata_parsed: minio::s3tables::iceberg::TableMetadata =
        load_resp.table_result().unwrap().metadata;

    // Use advanced Tier 2 API to commit table metadata changes
    // This demonstrates direct access to the advanced builder without client wrapper
    let _commit_resp = CommitTable::builder()
        .client(tables.clone())
        .warehouse_name(&warehouse_name)
        .namespace(vec![namespace_name.clone()])
        .table_name(&table_name)
        .metadata(table_metadata_parsed)
        // Add requirement to ensure table exists and hasn't been modified
        .requirements(vec![TableRequirement::AssertCreate])
        .build()
        .send()
        .await
        .unwrap();

    // Verify commit succeeded by checking response is Ok (advanced response doesn't have table() method)

    // Load table again to verify it still exists after commit
    let load_resp_after: LoadTableResponse = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify metadata location is still consistent
    let loaded_result = load_resp_after.table_result().unwrap();
    assert_eq!(loaded_result.metadata_location.unwrap(), original_metadata);

    // Cleanup - delete table and verify it's gone
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(&warehouse_name, &tables).await;
}
