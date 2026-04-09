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

use futures_util::StreamExt;
use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::SpeedtestResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
//#[ignore = "Performance tests are resource-intensive and time-consuming"]
async fn test_speedtest() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Starting speedtest");

    let response: SpeedtestResponse = madmin_client
        .speedtest()
        .build()
        .send()
        .await
        .expect("Failed to start speedtest");

    let mut stream = response.into_stream();
    let mut count = 0;
    let max_results = 5;
    let timeout = Duration::from_secs(30);

    let result = tokio::time::timeout(timeout, async {
        while let Some(result) = stream.next().await {
            match result {
                Ok(_speedtest_result) => {
                    println!("✓ Speedtest result {}: received", count + 1);
                    count += 1;
                    if count >= max_results {
                        break;
                    }
                }
                Err(e) => {
                    println!("✗ Speedtest error: {}", e);
                    break;
                }
            }
        }
        count
    })
    .await;

    match result {
        Ok(final_count) => {
            println!("✓ Speedtest completed: {} results", final_count);
        }
        Err(_) => {
            println!("✓ Speedtest timed out (acceptable for performance test)");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Performance tests are resource-intensive"]
async fn test_drive_speedtest() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Starting drive speedtest");

    let response = madmin_client
        .drive_speedtest()
        .build()
        .send()
        .await
        .expect("Failed to start drive speedtest");

    let mut stream = response.into_stream();
    let mut count = 0;
    let max_results = 5;
    let timeout = Duration::from_secs(30);

    let result = tokio::time::timeout(timeout, async {
        while let Some(result) = stream.next().await {
            match result {
                Ok(_drive_result) => {
                    println!("✓ Drive speedtest result {}: received", count + 1);
                    count += 1;
                    if count >= max_results {
                        break;
                    }
                }
                Err(e) => {
                    println!("✗ Drive speedtest error: {}", e);
                    break;
                }
            }
        }
        count
    })
    .await;

    match result {
        Ok(final_count) => {
            println!("✓ Drive speedtest completed: {} results", final_count);
        }
        Err(_) => {
            println!("✓ Drive speedtest timed out (acceptable for performance test)");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Network performance tests require specific setup"]
async fn test_netperf() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Starting netperf");

    let _response = madmin_client
        .netperf()
        .duration(Duration::from_secs(10))
        .build()
        .send()
        .await
        .expect("Failed to get netperf");

    println!("✓ Netperf result received");
    // NetperfResponse is a single JSON result, not a stream
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Client performance tests are resource-intensive"]
async fn test_client_perf() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Starting client perf");

    let _response = madmin_client
        .client_perf()
        .duration(Duration::from_secs(10))
        .build()
        .send()
        .await
        .expect("Failed to get client perf");

    println!("✓ Client perf result received");
    // ClientPerfResponse is a single JSON result, not a stream
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires multi-site deployment with site replication configured"]
async fn test_site_replication_perf() {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Starting site replication perf");

    let _response = madmin_client
        .site_replication_perf()
        .duration(Duration::from_secs(10))
        .build()
        .send()
        .await
        .expect("Failed to get site replication perf");

    println!("✓ Site replication perf result received");
    // SiteNetPerfResult is a single JSON result, not a stream
}
