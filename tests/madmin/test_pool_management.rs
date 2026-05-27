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

//! Integration tests for Pool Management APIs
//!
//! Tests the following APIs:
//! - ListPoolsStatus (get all pools and their decommission status)
//! - StatusPool (get individual pool status)
//! - DecommissionPool (start pool decommissioning)
//! - CancelDecommissionPool (cancel pool decommissioning)

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{ListPoolsStatusResponse, StatusPoolResponse};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

#[tokio::test]
async fn test_list_pools_status() {
    let madmin = get_madmin_client();

    // List all pools
    let pools_resp: ListPoolsStatusResponse = madmin
        .list_pools_status()
        .send()
        .await
        .expect("Failed to list pools");

    let pools = pools_resp.pools().unwrap();
    assert!(!pools.is_empty(), "Should have at least one pool");
    println!("Found {} pool(s)", pools.len());

    for pool in &pools {
        // Validate pool structure
        assert!(!pool.cmdline.is_empty(), "Pool cmdline should not be empty");

        println!("Pool ID: {}", pool.id);
        println!("  Command: {}", pool.cmdline);
        println!("  Last Update: {}", pool.last_update);

        if let Some(decom) = &pool.decommission {
            // Validate decommission data
            let progress = decom.percent_complete();
            assert!(
                (0.0..=100.0).contains(&progress),
                "Progress should be between 0 and 100"
            );

            println!("  Decommission Status:");
            println!("    Complete: {}", decom.complete);
            println!("    Failed: {}", decom.failed);
            println!("    Canceled: {}", decom.canceled);
            println!("    Progress: {:.2}%", progress);
            println!("    Objects Done: {}", decom.objects_decommissioned);
            println!("    Bytes Done: {}", decom.bytes_done);
        } else {
            println!("  Status: Active (not decommissioning)");
        }
    }
}

#[tokio::test]
async fn test_status_pool() {
    let madmin = get_madmin_client();

    // First get list of pools
    let pools_resp: ListPoolsStatusResponse = madmin
        .list_pools_status()
        .send()
        .await
        .expect("Failed to list pools");

    let pools = pools_resp.pools().unwrap();
    if pools.is_empty() {
        println!("No pools available, skipping status_pool test");
        return;
    }

    // Get status of first pool using its command line
    let pool_cmdline = &pools[0].cmdline;

    let status_resp: StatusPoolResponse = madmin
        .status_pool(pool_cmdline)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to get pool status");

    let status = status_resp.status().unwrap();
    assert!(
        !status.cmdline.is_empty(),
        "Pool cmdline should not be empty"
    );

    println!("Pool Status for: {}", pool_cmdline);
    println!("  Pool ID: {}", status.id);
    println!("  Last Update: {}", status.last_update);

    if let Some(decom) = &status.decommission {
        let progress = decom.percent_complete();
        assert!(
            (0.0..=100.0).contains(&progress),
            "Progress should be between 0 and 100"
        );

        println!("  Decommission Info:");
        println!("    Start Time: {}", decom.start_time);
        println!("    Total Size: {} bytes", decom.total_size);
        println!("    Current Size: {} bytes", decom.current_size);
        println!("    Progress: {:.2}%", progress);
    }

    assert_eq!(status.cmdline, *pool_cmdline);
}

#[tokio::test]
#[ignore]
async fn test_decommission_and_cancel_pool() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. It requires multiple pools to be configured
    // 2. Decommissioning can take a long time
    // 3. It affects the cluster's storage capacity
    //
    // To run this test:
    // - Ensure you have multiple pools configured
    // - Remove the #[ignore] attribute
    // - Run: cargo test test_decommission_and_cancel_pool -- --ignored --nocapture

    // Get list of pools
    let pools_resp: ListPoolsStatusResponse = madmin
        .list_pools_status()
        .send()
        .await
        .expect("Failed to list pools");

    let pools = pools_resp.pools().unwrap();
    if pools.len() < 2 {
        println!(
            "Test requires at least 2 pools, found {}. Skipping decommission test.",
            pools.len()
        );
        return;
    }

    // Find a pool that's not currently decommissioning
    let pool_to_decom = pools
        .iter()
        .find(|p| p.decommission.is_none())
        .expect("No available pool to decommission");

    let pool_cmdline = &pool_to_decom.cmdline;

    println!("Starting decommission for pool: {}", pool_cmdline);

    // Start decommissioning
    madmin
        .decommission_pool(pool_cmdline)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to start decommission");

    println!("Decommission started successfully");

    // Check status
    let status_resp: StatusPoolResponse = madmin
        .status_pool(pool_cmdline)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to get pool status");

    let status = status_resp.status().unwrap();
    assert!(
        status.decommission.is_some(),
        "Pool should be decommissioning"
    );

    // Cancel decommission immediately
    println!("Canceling decommission...");
    madmin
        .cancel_decommission_pool(pool_cmdline)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to cancel decommission");

    println!("Decommission canceled successfully");

    // Verify cancellation
    let status_resp: StatusPoolResponse = madmin
        .status_pool(pool_cmdline)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to get pool status");

    let status = status_resp.status().unwrap();
    if let Some(decom) = &status.decommission {
        assert!(decom.canceled, "Decommission should be marked as canceled");
    }
}

#[tokio::test]
async fn test_pool_decommission_info_percent() {
    let _madmin = get_madmin_client();
    use minio::madmin::types::pool_management::PoolDecommissionInfo;

    // Test percent_complete calculation
    let decom = PoolDecommissionInfo {
        start_time: chrono::Utc::now(),
        start_size: 1000,
        total_size: 1000,
        current_size: 500,
        complete: false,
        failed: false,
        canceled: false,
        objects_decommissioned: 50,
        objects_decommission_failed: 0,
        bytes_done: 500,
        bytes_failed: 0,
    };

    assert_eq!(decom.percent_complete(), 50.0);

    // Test edge case: total_size = 0
    let decom_zero = PoolDecommissionInfo {
        total_size: 0,
        ..decom
    };
    assert_eq!(decom_zero.percent_complete(), 0.0);

    // Test completed
    let decom_complete = PoolDecommissionInfo {
        current_size: 0,
        ..decom
    };
    assert_eq!(decom_complete.percent_complete(), 100.0);
}
