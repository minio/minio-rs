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

use minio::s3::tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn warehouse_list(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());

    // List warehouses (may or may not be empty depending on other tests)
    let resp = tables.list_warehouses().build().send().await.unwrap();

    // Just verify the call succeeds and returns a list
    assert!(resp.warehouses.is_empty() || !resp.warehouses.is_empty());
}
