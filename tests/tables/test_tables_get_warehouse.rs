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
async fn warehouse_get(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    // Create warehouse
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Get warehouse and verify fields
    let resp = tables
        .get_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name, warehouse_name);
    assert!(!resp.uuid.is_empty());
    assert!(resp.created_at.timestamp() > 0);

    // Cleanup
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
}
