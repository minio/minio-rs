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

//! Integration tests for Node Management APIs
//!
//! Tests the following APIs:
//! - Cordon (mark node as unschedulable)
//! - Uncordon (mark node as schedulable)
//! - Drain (drain node for maintenance)

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{CordonResponse, DrainResponse, UncordonResponse};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

fn get_test_node() -> String {
    let ctx = TestContext::new_from_env();
    format!("{}:{}", ctx.base_url.host(), ctx.base_url.port())
}

#[tokio::test]
#[ignore = "Node cordon/uncordon APIs are not supported in MinIO mode-server-xl (require distributed/erasure-code mode)"]
async fn test_cordon_uncordon_node() {
    let madmin = get_madmin_client();
    let node = get_test_node();

    println!("Testing node operations on: {}", node);

    // Test Cordon - mark node as unschedulable
    println!("Cordoning node...");
    let resp: CordonResponse = madmin
        .cordon(&node)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to cordon node");

    let result = resp.result().unwrap();
    assert!(!result.node.is_empty(), "Node should not be empty");
    for err in &result.errors {
        assert!(!err.is_empty(), "Error message should not be empty");
    }

    println!("Cordon result:");
    println!("  Target: {}", result.node);
    println!("  Errors: {}", result.errors.len());

    if !result.errors.is_empty() {
        for err in &result.errors {
            println!("    Error: {}", err);
        }
    }

    assert_eq!(result.node, node, "Should target the specified node");

    // Test Uncordon - mark node as schedulable again
    println!("Uncordoning node...");
    let resp: UncordonResponse = madmin
        .uncordon(&node)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to uncordon node");

    let result = resp.result().unwrap();
    assert!(!result.node.is_empty(), "Node should not be empty");
    for err in &result.errors {
        assert!(!err.is_empty(), "Error message should not be empty");
    }

    println!("Uncordon result:");
    println!("  Target: {}", result.node);
    println!("  Errors: {}", result.errors.len());

    if !result.errors.is_empty() {
        for err in &result.errors {
            println!("    Error: {}", err);
        }
    }

    assert_eq!(result.node, node, "Should target the specified node");
}

#[tokio::test]
#[ignore]
async fn test_drain_node() {
    // NOTE: This test is skipped by default because:
    // 1. Draining a node stops it from accepting new requests
    // 2. It can affect ongoing operations
    // 3. It's typically used during maintenance windows
    //
    // To run this test:
    // - Ensure you have a multi-node cluster
    // - Remove the skip attribute
    // - Run: cargo test test_drain_node -- --nocapture

    let madmin = get_madmin_client();
    let node = get_test_node();

    println!("Draining node: {}", node);

    let resp: DrainResponse = madmin
        .drain(&node)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to drain node");

    let result = resp.result().unwrap();
    assert!(!result.node.is_empty(), "Node should not be empty");
    for err in &result.errors {
        assert!(!err.is_empty(), "Error message should not be empty");
    }

    println!("Drain result:");
    println!("  Target: {}", result.node);
    println!("  Errors: {}", result.errors.len());

    if !result.errors.is_empty() {
        for err in &result.errors {
            println!("    Error: {}", err);
        }
    }

    // Note: After draining, you'd typically want to uncordon the node
    // to make it schedulable again after maintenance is complete
}

#[tokio::test]
async fn test_node_result_types() {
    use minio::madmin::types::node_management::CordonNodeResult;

    // Test CordonNodeResult construction
    let result = CordonNodeResult {
        node: "localhost:9000".to_string(),
        errors: vec![],
    };

    assert!(result.errors.is_empty());
    assert_eq!(result.node, "localhost:9000");

    // Test with errors
    let result_err = CordonNodeResult {
        node: "localhost:9000".to_string(),
        errors: vec!["Node not found".to_string()],
    };

    assert!(!result_err.errors.is_empty());
    assert_eq!(result_err.errors.len(), 1);
}
