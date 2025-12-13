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

use minio::s3::response::ListBucketsResponse;
use minio::s3::response_traits::HasS3Fields;
use minio::s3::types::{BucketName, S3Api};
use minio::s3tables::utils::WarehouseName;
use minio::s3tables::{TablesApi, TablesClient};
use minio_common::cleanup_guard::CleanupGuard;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn list_buckets(ctx: TestContext) {
    const N_BUCKETS: usize = 3;

    let mut names: Vec<String> = Vec::new();
    let mut guards: Vec<CleanupGuard> = Vec::new();
    for _ in 1..=N_BUCKETS {
        let (bucket_name, guard) = ctx.create_bucket_helper().await;
        names.push(bucket_name);
        guards.push(guard);
    }

    assert_eq!(names.len(), N_BUCKETS);

    let mut count = 0;
    let resp: ListBucketsResponse = ctx.client.list_buckets().build().send().await.unwrap();

    for bucket in resp.buckets().unwrap().iter() {
        if names.contains(&bucket.name.to_string()) {
            count += 1;
        }
        let n = &bucket.name;

        // Try to delete the bucket. If it fails because it's a warehouse bucket,
        // skip it (warehouse buckets must be deleted via the Tables API)
        println!("attempting to delete bucket: {}", n);
        match ctx
            .client
            .delete_and_purge_bucket(n.clone())
            .await
        {
            Ok(_) => {
                println!("  ✓ Successfully deleted S3 bucket: {}", n);
            }
            Err(e) => {
                // Check if this is a warehouse bucket deletion error
                let error_str = format!("{:?}", e);
                if error_str.contains("warehouse bucket") || error_str.contains("DeleteWarehouse") {
                    println!(
                        "  ⚠ Skipped warehouse bucket (use Tables API to delete): {}",
                        n
                    );
                } else {
                    // Some other error occurred - propagate it
                    panic!("Failed to delete S3 bucket {}: {}", n, e);
                }
            }
        }
    }

    assert_eq!(guards.len(), N_BUCKETS);
    assert_eq!(count, N_BUCKETS);
    for guard in guards {
        guard.cleanup().await;
    }
}

#[minio_macros::test(no_bucket)]
async fn warehouse_bucket_deletion_error_message(ctx: TestContext) {
    // This test verifies that attempting to delete a warehouse bucket produces a helpful error message
    let tables = TablesClient::builder()
        .endpoint(ctx.base_url.to_url_string())
        .credentials(&ctx.access_key, &ctx.secret_key)
        .region(ctx.base_url.region.clone())
        .build()
        .expect("Failed to create TablesClient");

    // Create a warehouse bucket
    let bucket_name = BucketName::new(format!(
        "test-wh-{}",
        uuid::Uuid::new_v4().to_string()[..8].to_lowercase()
    )).unwrap();
    let warehouse_name = WarehouseName::new(bucket_name.clone()).unwrap();

    let resp = tables
        .create_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Should create warehouse successfully");

    // Attempt to delete it using the regular S3 DeleteBucket API (which is incorrect)
    let delete_result = ctx
        .client
        .delete_and_purge_bucket(bucket_name)
        .await;

    // Should fail with a descriptive error message
    assert!(
        delete_result.is_err(),
        "Deleting warehouse bucket via S3 API should fail"
    );

    let error_msg = format!("{:?}", delete_result.err().unwrap());

    // Verify the error message is helpful and mentions the correct API
    assert!(
        error_msg.contains("warehouse bucket") && error_msg.contains("DeleteWarehouse"),
        "Error message should mention warehouse bucket and DeleteWarehouse API. Got: {}",
        error_msg
    );

    println!("Error message correctly identifies warehouse bucket issue:");
    println!("{}", error_msg);

    // Clean up the warehouse manually via the proper API
    let _ = tables.delete_warehouse(warehouse_name).build().send().await;
}

#[minio_macros::test(no_bucket)]
async fn warehouse_bucket_head_request_headers(ctx: TestContext) {
    // This test investigates whether a HEAD request on a warehouse bucket returns distinctive headers
    let tables = TablesClient::builder()
        .endpoint(ctx.base_url.to_url_string())
        .credentials(&ctx.access_key, &ctx.secret_key)
        .region(ctx.base_url.region.clone())
        .build()
        .expect("Failed to create TablesClient");

    // Create a warehouse bucket
    let warehouse_name_str = format!(
        "test-wh-head-{}",
        uuid::Uuid::new_v4().to_string()[..8].to_lowercase()
    );
    let warehouse_name = WarehouseName::try_from(warehouse_name_str.as_str())
        .expect("Failed to create WarehouseName");

    let resp = tables
        .create_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await;
    assert!(resp.is_ok(), "Should create warehouse successfully");

    // Try a HEAD request on the warehouse bucket
    let head_result = ctx
        .client
        .bucket_exists(BucketName::try_from(warehouse_name_str).unwrap())
        .build()
        .send()
        .await;

    println!("HEAD request result on warehouse bucket: {:?}", head_result);

    if let Ok(response) = head_result {
        println!("Bucket exists: true (HEAD succeeded)");
        println!("Response headers:");
        for (key, value) in response.headers().iter() {
            println!("  {}: {:?}", key, value);
        }
    } else if let Err(e) = head_result {
        println!("HEAD request error: {:?}", e);
        // Check if error message contains warehouse-related info
        let error_str = format!("{:?}", e);
        if error_str.contains("warehouse") || error_str.contains("Warehouse") {
            println!("Error message indicates warehouse bucket!");
        }
    }

    // Clean up the warehouse manually via the proper API
    let _ = tables.delete_warehouse(warehouse_name).build().send().await;
}
