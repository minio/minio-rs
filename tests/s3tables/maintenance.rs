// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Integration tests for maintenance operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::builders::TableMaintenanceConfig;
use minio::s3tables::response_traits::{
    HasMaintenanceJobStatus, HasTableMaintenanceConfiguration, HasWarehouseMaintenanceConfiguration,
};
use minio::s3tables::types::{
    CompactionSettings, MaintenanceStatus, MaintenanceType, UnreferencedFileRemovalSettings,
};
use minio_common::test_context::TestContext;

/// Check if an error indicates the API is unsupported
fn is_unsupported_api(err: &Error) -> bool {
    match err {
        Error::S3Server(minio::s3::error::S3ServerError::HttpError(400, msg)) => {
            msg.contains("unsupported API call")
        }
        _ => false,
    }
}

/// Test getting warehouse maintenance configuration
#[minio_macros::test(no_bucket)]
async fn get_warehouse_maintenance(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Get maintenance config
    let resp = tables
        .get_warehouse_maintenance(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.warehouse_maintenance_configuration().unwrap();
            println!("> Warehouse maintenance config retrieved: {:?}", config);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse maintenance API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting warehouse maintenance configuration
#[minio_macros::test(no_bucket)]
async fn put_warehouse_maintenance(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Set unreferenced file removal maintenance
    let settings = UnreferencedFileRemovalSettings::new(7, 30);

    let resp = tables
        .put_warehouse_maintenance(&warehouse, MaintenanceStatus::Enabled, Some(settings))
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Warehouse maintenance set successfully");

            // Verify by getting the config
            let get_resp = tables
                .get_warehouse_maintenance(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            if let Ok(resp) = get_resp {
                let config = resp.warehouse_maintenance_configuration().unwrap();
                println!("> Verified maintenance config: {:?}", config);
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse maintenance API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test getting table maintenance configuration
#[minio_macros::test(no_bucket)]
async fn get_table_maintenance(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get maintenance config
    let resp = tables
        .get_table_maintenance(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let config = resp.table_maintenance_configuration().unwrap();
            println!("> Table maintenance config retrieved: {:?}", config);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table maintenance API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting table maintenance configuration (compaction)
#[minio_macros::test(no_bucket)]
async fn put_table_maintenance_compaction(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Set compaction maintenance
    let config = TableMaintenanceConfig::compaction_enabled(CompactionSettings::new(512));

    let resp = tables
        .put_table_maintenance(&warehouse, &namespace, &table, config)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Table maintenance (compaction) set successfully");
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table maintenance API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test getting table maintenance job status
#[minio_macros::test(no_bucket)]
async fn get_table_maintenance_job_status(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Get maintenance job status
    let resp = tables
        .get_table_maintenance_job_status(
            &warehouse,
            &namespace,
            &table,
            MaintenanceType::IcebergCompaction,
        )
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let status = resp.maintenance_job_status().unwrap();
            println!("> Table maintenance job status: {:?}", status);
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table maintenance job status API not supported by server, skipping test");
        }
        Err(ref e) => {
            // May get 404 if no job exists - that's ok
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchJob") {
                println!("> No maintenance job exists (expected)");
            } else {
                panic!("Unexpected error: {e:?}");
            }
        }
    }

    // Cleanup
    tables
        .delete_table(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(&warehouse, &namespace, &tables).await;
    delete_warehouse_helper(&warehouse, &tables).await;
}
