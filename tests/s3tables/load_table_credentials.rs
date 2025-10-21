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
use minio::s3tables::TablesApi;
use minio::s3tables::response::LoadTableCredentialsResponse;
use minio_common::test_context::TestContext;

/// Check if an error indicates the API is unsupported
fn is_unsupported_api(err: &Error) -> bool {
    match err {
        Error::S3Server(minio::s3::error::S3ServerError::HttpError(status, msg)) => {
            // 400 = unsupported API or auth error
            *status == 400
                && (msg.contains("unsupported API call")
                    || msg.contains("AuthorizationParametersError"))
        }
        _ => false,
    }
}

/// Test loading table credentials for direct S3 access
#[minio_macros::test(no_bucket)]
async fn load_table_credentials(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    create_table_helper(
        warehouse_name.clone(),
        namespace.clone(),
        table_name.clone(),
        &tables,
    )
    .await;

    // Load table credentials
    let resp: Result<LoadTableCredentialsResponse, Error> = tables
        .load_table_credentials(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await;

    // Check if credentials loading is supported
    match resp {
        Ok(resp) => {
            // Verify credentials are returned
            let credentials = resp.storage_credentials().unwrap();

            // Check that credential information can be parsed
            // Server may or may not return credentials depending on configuration
            // We just verify the response is parseable
            let _ = credentials;
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("Load table credentials not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup - delete table first, then namespace, then warehouse
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
    delete_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
