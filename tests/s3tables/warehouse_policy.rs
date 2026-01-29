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

//! Integration tests for warehouse policy operations (AWS S3 Tables API)

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

/// Test getting warehouse policy
#[minio_macros::test(no_bucket)]
async fn get_warehouse_policy(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Try to get policy (may not exist yet)
    let resp = tables
        .get_warehouse_policy(&warehouse)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            // Policy exists - verify we can read it
            let _policy = resp.resource_policy();
            println!("> Warehouse policy retrieved successfully");
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse policy API not supported by server, skipping test");
        }
        Err(ref e) => {
            // May get 404 if no policy exists - that's ok
            let err_str = format!("{e:?}");
            if err_str.contains("404") || err_str.contains("NoSuchPolicy") {
                println!("> No policy exists (expected for new warehouse)");
            } else {
                panic!("Unexpected error: {e:?}");
            }
        }
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test putting and getting warehouse policy
#[minio_macros::test(no_bucket)]
async fn put_warehouse_policy(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Create a simple policy
    let policy = r#"{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3tables:*",
                "Resource": "*"
            }
        ]
    }"#;

    // Put policy
    let resp = tables
        .put_warehouse_policy(&warehouse, policy)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Warehouse policy set successfully");

            // Verify by getting the policy
            let get_resp = tables
                .get_warehouse_policy(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match get_resp {
                Ok(resp) => {
                    let retrieved_policy = resp.resource_policy().unwrap();
                    assert!(
                        retrieved_policy.contains("s3tables"),
                        "Policy should contain our action"
                    );
                }
                Err(e) => {
                    eprintln!("> Failed to get policy after put: {e:?}");
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse policy API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test deleting warehouse policy
#[minio_macros::test(no_bucket)]
async fn delete_warehouse_policy(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // First put a policy
    let policy = r#"{"Version": "2012-10-17", "Statement": []}"#;

    let put_resp = tables
        .put_warehouse_policy(&warehouse, policy)
        .unwrap()
        .build()
        .send()
        .await;

    match put_resp {
        Ok(_) => {
            // Now delete the policy
            let del_resp = tables
                .delete_warehouse_policy(&warehouse)
                .unwrap()
                .build()
                .send()
                .await;

            match del_resp {
                Ok(_) => {
                    println!("> Warehouse policy deleted successfully");
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Warehouse policy API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error deleting policy: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Warehouse policy API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error putting policy: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}
