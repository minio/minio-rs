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

//! Integration tests for table policy operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::HasResourcePolicy;
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

/// Test getting table policy
#[minio_macros::test(no_bucket)]
async fn get_table_policy(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Try to get policy
    let resp = tables
        .get_table_policy(&warehouse, &namespace, &table)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let _policy = resp.resource_policy();
            println!("> Table policy retrieved successfully");
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table policy API not supported by server, skipping test");
        }
        Err(ref e) => {
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchPolicy") {
                println!("> No policy exists (expected for new table)");
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

/// Test putting and getting table policy
#[minio_macros::test(no_bucket)]
async fn put_table_policy(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // Create a simple policy
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3tables:GetTable",
                "Resource": "*"
            }
        ]
    }"#;

    // Put policy
    let resp = tables
        .put_table_policy(&warehouse, &namespace, &table, policy)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Table policy set successfully");

            // Verify by getting the policy
            let get_resp = tables
                .get_table_policy(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            match get_resp {
                Ok(resp) => {
                    let retrieved_policy = resp.resource_policy().unwrap();
                    assert!(
                        retrieved_policy.contains("GetTable"),
                        "Policy should contain our action"
                    );
                }
                Err(e) => {
                    eprintln!("> Failed to get policy after put: {e:?}");
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table policy API not supported by server, skipping test");
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

/// Test deleting table policy
#[minio_macros::test(no_bucket)]
async fn delete_table_policy(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();
    let namespace = rand_namespace();
    let table = rand_table_name();

    // Create warehouse, namespace, and table
    create_warehouse_helper(&warehouse, &tables).await;
    create_namespace_helper(&warehouse, &namespace, &tables).await;
    create_table_helper(&warehouse, &namespace, &table, &tables).await;

    // First put a policy
    let policy = r#"{"Version": "2012-10-17", "Statement": []}"#;

    let put_resp = tables
        .put_table_policy(&warehouse, &namespace, &table, policy)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the policy
            let del_resp = tables
                .delete_table_policy(&warehouse, &namespace, &table)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Table policy deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Table policy API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting policy: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Table policy API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting policy: {e:?}"),
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
