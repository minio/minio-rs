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

//! Integration tests for Update Management APIs
//!
//! Tests the following APIs:
//! - ServerUpdate (trigger server update)
//! - CancelServerUpdate (cancel ongoing update)
//! - BumpVersion (bump configuration version)
//! - GetAPIDesc (get API version descriptions)

use minio::madmin::builders::ServerUpdate;
use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{
    BumpVersionResponse, CancelServerUpdateResponse, GetAPIDescResponse, ServerUpdateResponse,
};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

#[tokio::test]
#[ignore = "API description endpoint is not supported in MinIO mode-server-xl (standard deployment mode)"]
async fn test_get_api_desc() {
    let madmin = get_madmin_client();

    let desc_resp: GetAPIDescResponse = madmin
        .get_api_desc()
        .send()
        .await
        .expect("Failed to get API description");

    let desc = desc_resp.description().unwrap();
    println!("Cluster API Description:");

    if let Some(nodes) = &desc.nodes {
        assert!(!nodes.is_empty(), "Nodes map should not be empty");

        for (node_name, api_desc) in nodes {
            assert!(!node_name.is_empty(), "Node name should not be empty");

            println!("  Node: {}", node_name);
            println!("    Backend Version: {:?}", api_desc.backend_version);
            println!("    Node API Version: {}", api_desc.node_api_version);

            if let Some(error) = &api_desc.error {
                assert!(!error.is_empty(), "Error should not be empty string");
                println!("    Error: {}", error);
            }
        }

        println!("  Found {} node(s)", nodes.len());
    }

    if let Some(ref error) = desc.error {
        println!("  Error: {}", error);
    }
}

#[tokio::test]
#[ignore]
async fn test_server_update_dry_run() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Server updates can restart the server
    // 2. May require specific MinIO versions
    // 3. Can disrupt service
    //
    // To run this test:
    // - Remove the skip attribute
    // - Run: cargo test test_server_update_dry_run -- --nocapture

    // Test with dry-run to see what would happen without actually updating
    println!("Testing server update (dry-run)...");

    let update_url = "https://dl.min.io/server/minio/release/linux-amd64/minio";

    use minio::madmin::builders::ServerUpdate;
    let update_resp: ServerUpdateResponse = ServerUpdate::builder()
        .client(madmin.clone())
        .update_url(Some(update_url.to_string()))
        .dry_run(true)
        .build()
        .send()
        .await
        .expect("Failed to perform dry-run update");

    let result = update_resp.status().unwrap();
    println!("Dry-run update result:");
    println!("  Dry Run: {}", result.dry_run);

    if let Some(ref results) = result.results {
        assert!(!results.is_empty(), "Results should not be empty");

        for node_result in results {
            assert!(!node_result.host.is_empty(), "Host should not be empty");
            assert!(
                !node_result.current_version.is_empty(),
                "Current version should not be empty"
            );

            if let Some(ref error) = node_result.err {
                assert!(!error.is_empty(), "Error should not be empty string");
            }
        }

        println!("  Results from {} node(s):", results.len());
        for node_result in results {
            println!("    Host: {}", node_result.host);
            println!("      Current Version: {}", node_result.current_version);

            if let Some(ref error) = node_result.err {
                println!("      Error: {}", error);
            }
        }
    }

    if let Some(ref error) = result.error {
        println!("  Overall Error: {}", error);
    }

    assert!(result.dry_run, "Should be a dry-run");
}

#[tokio::test]
#[ignore]
async fn test_server_update_and_cancel() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. It attempts to actually update the server
    // 2. Can restart services
    // 3. Requires careful setup
    //
    // To run this test:
    // - Remove the skip attribute
    // - Ensure you have a test cluster
    // - Run: cargo test test_server_update_and_cancel -- --nocapture

    let update_url = "https://dl.min.io/server/minio/release/linux-amd64/minio";

    println!("Starting server update (will cancel immediately)...");

    // Note: In practice, you'd start the update, then cancel it before it completes
    // This is a simplified test that just verifies the API calls work

    // Start update
    let _result: ServerUpdateResponse = ServerUpdate::builder()
        .client(madmin.clone())
        .update_url(Some(update_url.to_string()))
        .build()
        .send()
        .await
        .expect("Failed to start update");

    // Cancel update immediately
    println!("Canceling update...");
    let _cancel: CancelServerUpdateResponse = madmin
        .cancel_server_update()
        .send()
        .await
        .expect("Failed to cancel update");

    println!("Update canceled successfully");
}

#[tokio::test]
#[ignore]
async fn test_bump_version() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Bumping version changes cluster state
    // 2. May affect compatibility
    // 3. Should only be done when needed
    //
    // To run this test:
    // - Remove the skip attribute
    // - Run: cargo test test_bump_version -- --nocapture

    println!("Bumping cluster version...");

    let bump_resp: BumpVersionResponse = madmin
        .bump_version()
        .send()
        .await
        .expect("Failed to bump version");

    let result = bump_resp.result().unwrap();
    println!("Bump version result:");

    if let Some(ref nodes) = result.nodes {
        assert!(!nodes.is_empty(), "Nodes map should not be empty");

        for (node_name, node_result) in nodes.iter() {
            assert!(!node_name.is_empty(), "Node name should not be empty");

            if let Some(ref error) = node_result.error {
                assert!(!error.is_empty(), "Error should not be empty string");
            }
        }

        println!("  Updated {} node(s)", nodes.len());

        for (node_name, node_result) in nodes.iter() {
            println!("  Node: {}", node_name);
            println!("    Done: {}", node_result.done);
            println!("    Offline: {}", node_result.offline);

            if let Some(ref error) = node_result.error {
                println!("    Error: {}", error);
            }
        }
    }

    if let Some(ref error) = result.error {
        println!("  Overall Error: {}", error);
    }
}

#[tokio::test]
async fn test_update_types() {
    let _madmin = get_madmin_client();
    use minio::madmin::types::update::{NodeBumpVersionResp, ServerPeerUpdateStatus};

    // Test ServerPeerUpdateStatus
    let status = ServerPeerUpdateStatus {
        host: "localhost:9000".to_string(),
        current_version: "RELEASE.2024-11-07".to_string(),
        updated_version: "RELEASE.2024-11-08".to_string(),
        err: None,
        waiting_drives: None,
    };

    assert_eq!(status.host, "localhost:9000");
    assert_eq!(status.updated_version, "RELEASE.2024-11-08");
    assert!(status.err.is_none());

    // Test NodeBumpVersionResp
    let bump_resp = NodeBumpVersionResp {
        done: true,
        offline: false,
        error: None,
    };

    assert!(bump_resp.done);
    assert!(!bump_resp.offline);
    assert!(bump_resp.error.is_none());
}

#[tokio::test]
#[ignore = "ServerUpdate can restart the server"]
async fn test_server_update() {
    let madmin = get_madmin_client();

    // Test basic server update functionality
    let update_url = "https://dl.min.io/server/minio/release/linux-amd64/minio";

    println!("Testing server update...");

    let update_resp: ServerUpdateResponse = ServerUpdate::builder()
        .client(madmin.clone())
        .update_url(Some(update_url.to_string()))
        .build()
        .send()
        .await
        .expect("Failed to perform server update");

    let result = update_resp.status().unwrap();
    println!("Server update result:");
    println!("  Dry Run: {}", result.dry_run);

    if let Some(ref results) = result.results {
        println!("  Results from {} node(s):", results.len());
        for node_result in results {
            println!("    Host: {}", node_result.host);
            println!("      Current Version: {}", node_result.current_version);

            if !node_result.updated_version.is_empty() {
                println!("      Updated Version: {}", node_result.updated_version);
            }

            if let Some(ref error) = node_result.err {
                println!("      Error: {}", error);
            }

            if let Some(ref waiting) = node_result.waiting_drives {
                println!("      Waiting Drives: {:?}", waiting);
            }
        }
    }

    if let Some(ref error) = result.error {
        println!("  Overall Error: {}", error);
    }

    println!("✓ ServerUpdate API call successful");
}

#[tokio::test]
#[ignore = "ServerUpdate requires specific MinIO version setup"]
async fn test_server_update_with_options() {
    let madmin = get_madmin_client();

    // Test server update with additional options
    println!("Testing server update with options...");

    let update_resp: ServerUpdateResponse = ServerUpdate::builder()
        .client(madmin.clone())
        .update_url(Some(
            "https://dl.min.io/server/minio/release/linux-amd64/minio".to_string(),
        ))
        .dry_run(true)
        .build()
        .send()
        .await
        .expect("Failed to perform server update");

    let result = update_resp.status().unwrap();
    println!("Server update with options result:");

    assert!(result.dry_run, "Should be a dry-run update");

    if let Some(ref results) = result.results {
        for node_result in results {
            println!("  Node: {}", node_result.host);
            println!("    Current Version: {}", node_result.current_version);
        }
    }

    println!("✓ ServerUpdate with options successful");
}
