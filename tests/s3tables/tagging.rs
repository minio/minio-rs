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

//! Integration tests for tagging operations (AWS S3 Tables API)

use super::common::*;
use minio::s3::error::Error;
use minio::s3tables::TablesApi;
use minio::s3tables::response_traits::HasTags;
use minio::s3tables::types::Tag;
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

/// Test tagging a warehouse resource
#[minio_macros::test(no_bucket)]
async fn tag_warehouse_resource(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    // Create tags
    let tags = vec![
        Tag::new("Environment", "Test"),
        Tag::new("Team", "Engineering"),
    ];

    // The resource ARN would typically be the warehouse ARN
    // For testing, we use a placeholder format
    let resource_arn = format!(
        "arn:aws:s3tables:us-east-1:123456789012:bucket/{}",
        warehouse.as_str()
    );

    // Tag the resource
    let resp = tables
        .tag_resource(&resource_arn, tags.clone())
        .build()
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("> Resource tagged successfully");

            // List tags to verify
            let list_resp = tables
                .list_tags_for_resource(&resource_arn)
                .build()
                .send()
                .await;

            match list_resp {
                Ok(resp) => {
                    let retrieved_tags = resp.tags().unwrap();
                    assert!(!retrieved_tags.is_empty(), "Should have tags");
                    println!("> Retrieved {} tags", retrieved_tags.len());
                }
                Err(e) => {
                    eprintln!("> Failed to list tags: {e:?}");
                }
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Tagging API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test listing tags for a resource
#[minio_macros::test(no_bucket)]
async fn list_tags_for_resource(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    let resource_arn = format!(
        "arn:aws:s3tables:us-east-1:123456789012:bucket/{}",
        warehouse.as_str()
    );

    // List tags (may be empty for new resource)
    let resp = tables
        .list_tags_for_resource(&resource_arn)
        .build()
        .send()
        .await;

    match resp {
        Ok(resp) => {
            let tags = resp.tags().unwrap();
            println!("> Listed {} tags for resource", tags.len());
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Tagging API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}

/// Test untagging a resource
#[minio_macros::test(no_bucket)]
async fn untag_resource(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Create warehouse
    create_warehouse_helper(&warehouse, &tables).await;

    let resource_arn = format!(
        "arn:aws:s3tables:us-east-1:123456789012:bucket/{}",
        warehouse.as_str()
    );

    // First add some tags
    let tags = vec![
        Tag::new("Environment", "Test"),
        Tag::new("Team", "Engineering"),
    ];

    let tag_resp = tables
        .tag_resource(&resource_arn, tags)
        .build()
        .send()
        .await;

    match tag_resp {
        Ok(_) => {
            // Now remove one tag
            let untag_resp = tables
                .untag_resource(&resource_arn, vec!["Environment".to_string()])
                .build()
                .send()
                .await;

            match untag_resp {
                Ok(_) => {
                    println!("> Tag removed successfully");

                    // Verify by listing tags
                    let list_resp = tables
                        .list_tags_for_resource(&resource_arn)
                        .build()
                        .send()
                        .await;

                    if let Ok(resp) = list_resp {
                        let remaining_tags = resp.tags().unwrap();
                        // Should only have "Team" tag now
                        for tag in &remaining_tags {
                            assert_ne!(
                                tag.key(),
                                "Environment",
                                "Environment tag should be removed"
                            );
                        }
                    }
                }
                Err(ref e) if is_unsupported_api(e) => {
                    eprintln!("> Tagging API not supported by server, skipping test");
                }
                Err(e) => panic!("Unexpected error untagging: {e:?}"),
            }
        }
        Err(ref e) if is_unsupported_api(e) => {
            eprintln!("> Tagging API not supported by server, skipping test");
        }
        Err(e) => panic!("Unexpected error tagging: {e:?}"),
    }

    // Cleanup
    delete_warehouse_helper(&warehouse, &tables).await;
}
