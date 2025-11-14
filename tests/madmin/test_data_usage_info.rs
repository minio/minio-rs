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
use minio::madmin::response::DataUsageInfoResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_data_usage_info() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: DataUsageInfoResponse = madmin_client
        .data_usage_info()
        .build()
        .send()
        .await
        .expect("Failed to get data usage info");

    assert!(
        resp.info.last_update.timestamp() > 0,
        "Last update timestamp should be positive"
    );
    println!("✓ Last updated: {}", resp.info.last_update);
    println!("✓ Objects count: {}", resp.info.objects_count);
    println!(
        "✓ Objects total size: {} bytes",
        resp.info.objects_total_size
    );
    println!("✓ Buckets count: {}", resp.info.buckets_count);

    if let Some(capacity) = resp.info.capacity {
        println!("✓ Capacity: {} bytes", capacity);
    }

    if let Some(free_capacity) = resp.info.free_capacity {
        println!("✓ Free capacity: {} bytes", free_capacity);
    }

    if let Some(used_capacity) = resp.info.used_capacity {
        println!("✓ Used capacity: {} bytes", used_capacity);
    }

    if let Some(buckets_usage) = &resp.info.buckets_usage {
        println!(
            "✓ Per-bucket usage available for {} buckets",
            buckets_usage.len()
        );
        for (bucket_name, usage) in buckets_usage.iter().take(3) {
            println!(
                "  Bucket '{}': {} objects, {} bytes",
                bucket_name, usage.objects_count, usage.size
            );
        }
    }

    if let Some(tier_stats) = &resp.info.tier_stats
        && !tier_stats.is_empty()
    {
        println!("✓ Tier statistics available for {} tiers", tier_stats.len());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_data_usage_info_without_capacity() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: DataUsageInfoResponse = madmin_client
        .data_usage_info()
        .include_capacity(false)
        .build()
        .send()
        .await
        .expect("Failed to get data usage info");

    assert!(
        resp.info.last_update.timestamp() > 0,
        "Last update timestamp should be positive"
    );
    println!("✓ Objects count: {}", resp.info.objects_count);
    println!("✓ Buckets count: {}", resp.info.buckets_count);
}
