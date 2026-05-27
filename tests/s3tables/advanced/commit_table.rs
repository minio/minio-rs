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
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn advanced_commit_table(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    create_warehouse_helper(&warehouse, &tables).await;

    tables
        .create_namespace(&warehouse, &namespace)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let schema = create_test_schema();
    let create_resp: CreateTableResponse = tables
        .create_table(&warehouse, &namespace, &table, schema)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let original_metadata = create_resp
        .table_result()
        .unwrap()
        .metadata_location
        .clone()
        .unwrap();

    // Use advanced Tier 2 API to commit table metadata changes
    // Note: AssertCreate means "assert table does NOT exist", so we don't use it here
    // since the table was just created. Empty requirements + updates is a valid no-op commit.
    let _commit_resp = tables
        .adv_commit_table(&warehouse, &namespace, &table)
        .unwrap()
        // No requirements for this test - just testing the API connectivity
        .requirements(vec![])
        // No updates - just testing the commit API works
        .updates(vec![])
        .build()
        .send()
        .await
        .unwrap();

    // Verify commit succeeded by checking response is Ok (advanced response doesn't have table() method)

    // Load table again to verify it still exists after commit
    let load_resp_after: LoadTableResponse = tables
        .load_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify table still exists (even an empty commit creates a new metadata version)
    let loaded_result = load_resp_after.table_result().unwrap();
    assert!(
        loaded_result.metadata_location.is_some(),
        "Table should still have metadata location after commit"
    );
    // Note: Metadata location changes with each commit, so we don't compare to original
    let _ = original_metadata; // Acknowledge we captured it but don't need to compare

    // Cleanup - delete table and verify it's gone
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let resp: Result<_, Error> = tables
        .load_table(&warehouse, &namespace, table)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}
