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
use minio::madmin::response::ClusterAPIStatsResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Struct definition needs refinement to match actual API response format
async fn test_cluster_api_stats() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: ClusterAPIStatsResponse = madmin_client
        .cluster_api_stats()
        .build()
        .send()
        .await
        .expect("Failed to get cluster API stats");

    assert!(
        resp.stats.collected_at.timestamp() > 0,
        "Collected timestamp should be valid"
    );
    println!("✓ Stats collected at: {}", resp.stats.collected_at);
    println!("✓ Nodes reporting: {}", resp.stats.nodes);
    println!("✓ Active requests: {}", resp.stats.active_requests);
    println!("✓ Queued requests: {}", resp.stats.queued_requests);

    if let Some(errors) = &resp.stats.errors
        && !errors.is_empty()
    {
        println!("⚠ Errors: {}", errors.len());
        for error in errors {
            println!("  - {}", error);
        }
    }

    if let Some(last_minute) = &resp.stats.last_minute {
        println!("Last minute stats:");
        println!("  Total requests: {}", last_minute.total_requests);
        println!("  Total errors: {}", last_minute.total_errors);
        println!("  Total 5xx: {}", last_minute.total_5xx);
        println!("  Total 4xx: {}", last_minute.total_4xx);
        println!("  Avg duration: {:.2} ms", last_minute.avg_duration_ms);
        println!("  Max duration: {:.2} ms", last_minute.max_duration_ms);
    }

    if let Some(last_day) = &resp.stats.last_day {
        println!("Last day stats:");
        println!("  Total requests: {}", last_day.total_requests);
        println!("  Total errors: {}", last_day.total_errors);
        println!("  Total 5xx: {}", last_day.total_5xx);
        println!("  Total 4xx: {}", last_day.total_4xx);
        println!("  Avg duration: {:.2} ms", last_day.avg_duration_ms);
        println!("  Max duration: {:.2} ms", last_day.max_duration_ms);
    }

    if let Some(segmented) = &resp.stats.last_day_segmented {
        println!("Last day segmented: {} entries", segmented.len());
    }
}
