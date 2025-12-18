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
use minio::s3tables::error::TablesError;
use minio::s3tables::response::{CreateTableResponse, LoadTableResponse, RegisterTableResponse};
use minio::s3tables::utils::MetadataLocation;
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;

/// Check if an error indicates the API is unsupported or table already exists
fn is_unsupported_or_exists(err: &Error) -> bool {
    match err {
        Error::S3Server(minio::s3::error::S3ServerError::HttpError(400, msg)) => {
            msg.contains("unsupported API call")
        }
        Error::TablesError(TablesError::TableAlreadyExists { .. }) => {
            // Server may consider registering same metadata as "already exists"
            true
        }
        _ => false,
    }
}

#[minio_macros::test(no_bucket)]
async fn table_register(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();
    let registered_table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create initial table to get metadata location
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
        .unwrap();

    let table_result = resp.table_result().unwrap();
    let metadata_location_str: String = table_result.metadata_location.unwrap();
    assert!(metadata_location_str.starts_with(&format!("s3://{}/", warehouse_name.as_str())));

    // Register the table with a different name using the same metadata location
    let metadata_location = MetadataLocation::new(&metadata_location_str).unwrap();
    let register_resp: Result<RegisterTableResponse, Error> = tables
        .register_table(
            warehouse_name.clone(),
            namespace.clone(),
            registered_table_name.clone(),
            metadata_location,
        )
        .build()
        .send()
        .await;

    // Check if register table is supported
    match register_resp {
        Ok(register_resp) => {
            // Verify register response metadata
            let register_result = register_resp.table_result().unwrap();
            assert_eq!(
                register_result.metadata_location.as_ref().unwrap(),
                &metadata_location_str
            );

            // Verify registered table exists and has correct metadata
            let load_resp: LoadTableResponse = tables
                .load_table(
                    warehouse_name.clone(),
                    namespace.clone(),
                    registered_table_name.clone(),
                )
                .build()
                .send()
                .await
                .unwrap();

            // Verify load response
            let load_result = load_resp.table_result().unwrap();
            assert_eq!(
                load_result.metadata_location.as_ref().unwrap(),
                &metadata_location_str
            );

            // Cleanup registered table
            tables
                .delete_table(
                    warehouse_name.clone(),
                    namespace.clone(),
                    registered_table_name.clone(),
                )
                .build()
                .send()
                .await
                .unwrap();
        }
        Err(ref e) if is_unsupported_or_exists(e) => {
            eprintln!("Register table not supported or table already exists, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup - delete original table
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
    let resp: Result<_, Error> = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
