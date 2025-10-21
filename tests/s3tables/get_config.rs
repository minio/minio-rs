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
use minio::s3tables::response::GetConfigResponse;
use minio::s3tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn config_get(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Get config and verify all properties
    let resp: GetConfigResponse = tables
        .get_config(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify response content - CatalogConfig structure is accessible
    let config = resp.catalog_config().unwrap();
    // Access config fields to verify they exist (may be empty or populated)
    let _ = (&config.defaults, &config.overrides, &config.endpoints);

    delete_warehouse_helper(warehouse_name, &tables).await;
}
