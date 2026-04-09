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

//! Integration tests for Rebalancing APIs
//!
//! Tests the following APIs:
//! - RebalanceStart (start cluster rebalance)
//! - RebalanceStatus (check rebalance progress)
//! - RebalanceStop (stop active rebalance)

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{RebalanceStartResponse, RebalanceStatusResponse};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

#[tokio::test]
async fn test_rebalance_status() {
    let madmin = get_madmin_client();

    // Check rebalance status (should work even if no rebalance is running)
    let status_result: Result<RebalanceStatusResponse, _> =
        madmin.rebalance_status().build().send().await;

    // Handle case where rebalance is not started
    match status_result {
        Ok(status_resp) => {
            let status = status_resp.status().unwrap();
            assert!(!status.id.is_empty(), "Rebalance ID should not be empty");

            for pool in &status.pools {
                assert!(!pool.status.is_empty(), "Pool status should not be empty");
                assert!(
                    pool.used >= 0.0 && pool.used <= 100.0,
                    "Pool usage should be between 0 and 100"
                );

                if let Some(ref _progress) = pool.progress {}
            }

            println!("Rebalance Status:");
            println!("  ID: {}", status.id);

            if let Some(ref stopped_at) = status.stopped_at {
                println!("  Stopped at: {}", stopped_at);
            }
            println!("✓ Successfully retrieved rebalance status");
        }
        Err(e) => {
            // Check if error is "rebalance not started" which is acceptable
            let err_str = format!("{:?}", e);
            if err_str.contains("XMinioAdminRebalanceNotStarted")
                || err_str.contains("rebalance is not started")
            {
                println!("✓ Rebalance is not started (this is expected)");
            } else {
                panic!("Failed to get rebalance status: {:?}", e);
            }
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_rebalance_start_and_stop() {
    let madmin = get_madmin_client();

    // NOTE: This test is skipped by default because:
    // 1. Rebalancing moves data across pools
    // 2. It can be a long-running operation
    // 3. It impacts cluster performance
    // 4. Requires multiple pools to be effective
    //
    // To run this test:
    // - Ensure you have multiple pools configured
    // - Run: cargo test test_rebalance_start_and_stop -- --ignored --nocapture

    // Check if rebalance is already running
    let initial_status_resp: RebalanceStatusResponse = madmin
        .rebalance_status()
        .build()
        .send()
        .await
        .expect("Failed to get initial rebalance status");

    let initial_status = initial_status_resp.status().unwrap();
    let is_running = initial_status.stopped_at.is_none()
        && initial_status.pools.iter().any(|p| p.status == "Active");

    if is_running {
        println!("Rebalance already running, stopping it first...");
        madmin
            .rebalance_stop()
            .build()
            .send()
            .await
            .expect("Failed to stop existing rebalance");

        // Wait a moment for it to stop
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    // Start rebalance
    println!("Starting rebalance...");
    let result: RebalanceStartResponse = madmin
        .rebalance_start()
        .build()
        .send()
        .await
        .expect("Failed to start rebalance");

    println!("Rebalance started:");
    println!("  Operation ID: {}", result.id().unwrap());

    // Check status
    let status_resp: RebalanceStatusResponse = madmin
        .rebalance_status()
        .build()
        .send()
        .await
        .expect("Failed to get rebalance status");

    let status = status_resp.status().unwrap();
    println!("Status after start:");
    println!("  ID: {}", status.id);
    println!("  Stopped: {}", status.stopped_at.is_some());

    // Stop rebalance
    println!("Stopping rebalance...");
    madmin
        .rebalance_stop()
        .build()
        .send()
        .await
        .expect("Failed to stop rebalance");

    println!("Rebalance stopped successfully");

    // Verify it stopped
    let final_status_resp = madmin
        .rebalance_status()
        .build()
        .send()
        .await
        .expect("Failed to get final rebalance status");

    let final_status = final_status_resp.status().unwrap();
    println!("Final status:");
    println!("  Stopped: {}", final_status.stopped_at.is_some());
}

#[tokio::test]
async fn test_rebalance_types() {
    use minio::madmin::types::rebalance::{
        RebalPoolProgress, RebalancePoolStatus, RebalanceStatus,
    };

    // Test RebalPoolProgress
    let pool_progress = RebalPoolProgress {
        num_objects: 1000,
        num_versions: 2000,
        bytes: 1024 * 1024 * 100, // 100MB
        bucket: "test-bucket".to_string(),
        object: "test-object".to_string(),
        elapsed_nanos: 30_000_000_000, // 30 seconds
        eta_nanos: 60_000_000_000,     // 60 seconds
    };

    assert_eq!(pool_progress.num_objects, 1000);
    assert_eq!(pool_progress.bytes, 1024 * 1024 * 100);
    assert_eq!(pool_progress.elapsed().as_secs(), 30);
    assert_eq!(pool_progress.eta().as_secs(), 60);

    // Test RebalancePoolStatus
    let pool_status = RebalancePoolStatus {
        id: 0,
        status: "Active".to_string(),
        used: 75.5,
        progress: Some(pool_progress),
    };

    assert_eq!(pool_status.id, 0);
    assert_eq!(pool_status.status, "Active");
    assert!(pool_status.progress.is_some());

    // Test RebalanceStatus
    let status = RebalanceStatus {
        id: "rebalance-123".to_string(),
        stopped_at: None,
        pools: vec![pool_status],
    };

    assert_eq!(status.id, "rebalance-123");
    assert!(status.stopped_at.is_none());
    assert_eq!(status.pools.len(), 1);
}
