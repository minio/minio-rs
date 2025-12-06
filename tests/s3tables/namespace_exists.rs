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
use minio::s3tables::TablesApi;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn namespace_exists_check(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Check if namespace exists before creation (should return exists=false)
    let resp = tables
        .namespace_exists(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .expect("namespace_exists should not return error for non-existent namespace");
    assert!(
        !resp.exists(),
        "Namespace should not exist before creation (exists() should return false)"
    );

    // Create the namespace
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Now check if namespace exists (should return exists=true)
    let resp = tables
        .namespace_exists(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .expect("namespace_exists should succeed");
    assert!(
        resp.exists(),
        "Namespace should exist after creation (exists() should return true)"
    );

    // Delete namespace
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Check if namespace exists after deletion (should return exists=false)
    let resp = tables
        .namespace_exists(warehouse_name.clone(), namespace)
        .build()
        .send()
        .await
        .expect("namespace_exists should not return error for deleted namespace");
    assert!(
        !resp.exists(),
        "Namespace should not exist after deletion (exists() should return false)"
    );

    delete_warehouse_helper(warehouse_name, &tables).await;
}
