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
use minio::s3::error::Error;
use minio::s3tables::response::{DeleteWarehouseResponse, ListWarehousesResponse};
use minio::s3tables::utils::WarehouseName;
use minio::s3tables::{HasPagination, TablesApi};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn list_warehouses(ctx: TestContext) {
    const N_WAREHOUSES: usize = 3;

    let tables = create_tables_client(&ctx);
    let mut created_names: Vec<WarehouseName> = Vec::new();

    // Create test warehouses
    for i in 1..=N_WAREHOUSES {
        let warehouse_name_str = format!(
            "test-wh-{}-{}",
            i,
            uuid::Uuid::new_v4().to_string()[..8].to_lowercase()
        );
        let warehouse = match WarehouseName::try_from(warehouse_name_str.as_str()) {
            Ok(name) => name,
            Err(e) => panic!("Failed to create warehouse name: {:?}", e),
        };

        match tables
            .create_warehouse(&warehouse)
            .unwrap()
            .build()
            .send()
            .await
        {
            Ok(_) => {
                created_names.push(warehouse);
            }
            Err(e) => {
                panic!("Warehouse creation failed: {:?}", e);
            }
        }
    }

    // List all warehouses
    let resp: ListWarehousesResponse = tables.list_warehouses().build().send().await.unwrap();
    let warehouse_names = resp.warehouses().unwrap();

    // Clean up test and chaos warehouses
    for warehouse in warehouse_names.iter() {
        // Delete chaos warehouses (cleanup from previous runs)
        let _resp: Result<DeleteWarehouseResponse, Error> =
            tables.delete_and_purge_warehouse(warehouse).await;

        // Delete our test warehouses
        if warehouse.as_str().starts_with("test-wh-") {
            let _ = tables
                .delete_warehouse(warehouse)
                .unwrap()
                .build()
                .send()
                .await;
        }
    }

    // Verify pagination token method works
    let _next_token = resp.next_token().unwrap_or(None);
}
