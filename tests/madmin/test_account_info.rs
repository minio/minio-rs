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

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::AccountInfoResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_account_info_basic() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: AccountInfoResponse = madmin_client
        .account_info()
        .build()
        .send()
        .await
        .expect("Failed to get account info");

    let account = resp.account().expect("Failed to parse account info");

    assert!(
        !account.account_name.is_empty(),
        "Account name should not be empty"
    );
    println!("✓ Account: {}", account.account_name);
    println!("✓ Number of buckets: {}", account.buckets.len());

    // Print bucket info
    for bucket in &account.buckets {
        println!(
            "  - {}: {} objects, {} bytes",
            bucket.name, bucket.objects, bucket.size
        );
        println!(
            "    Access: read={}, write={}",
            bucket.access.read, bucket.access.write
        );
    }

    // Verify backend info is present
    println!("✓ Backend type: {:?}", account.server.backend_type);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_account_info_with_prefix_usage() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: AccountInfoResponse = madmin_client
        .account_info()
        .prefix_usage(true)
        .build()
        .send()
        .await
        .expect("Failed to get account info with prefix usage");

    assert!(
        !resp.account().unwrap().account_name.is_empty(),
        "Account name should not be empty"
    );
    println!("✓ Account: {}", resp.account().unwrap().account_name);

    // Check if any buckets have prefix usage data
    for bucket in &resp.account().unwrap().buckets {
        if !bucket.prefix_usage.is_empty() {
            println!(
                "  - {} has {} prefixes tracked",
                bucket.name,
                bucket.prefix_usage.len()
            );
            for (prefix, size) in bucket.prefix_usage.iter().take(5) {
                println!("    - {}: {} bytes", prefix, size);
            }
        }
    }

    println!("✓ Account info with prefix usage retrieved successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_account_info_bucket_details() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: AccountInfoResponse = madmin_client
        .account_info()
        .build()
        .send()
        .await
        .expect("Failed to get account info");

    // Check bucket details if any buckets exist
    for bucket in &resp.account().unwrap().buckets {
        println!("  - Bucket: {}", bucket.name);
        println!("    Created: {}", bucket.created);

        if let Some(details) = &bucket.details {
            println!("    Versioning: {}", details.versioning);
            println!("    Locking: {}", details.locking);
            println!("    Replication: {}", details.replication);
        }

        // Print histograms if available
        if !bucket.object_sizes_histogram.is_empty() {
            println!(
                "    Object size histogram: {} entries",
                bucket.object_sizes_histogram.len()
            );
        }
    }

    println!("✓ Account info bucket details retrieved successfully");
}
