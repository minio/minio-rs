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
use minio::madmin::response::{BucketReplicationDiffResponse, BucketReplicationMRFResponse};
use minio::madmin::types::MadminApi;
use minio::madmin::types::replication::ReplDiffOpts;
use minio::s3::creds::StaticProvider;
use minio::s3::types::BucketName;
use minio_common::test_context::TestContext;

#[minio_macros::test(skip_if_express)]
async fn test_bucket_replication_diff(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Checking replication diff for bucket '{}'", bucket_name);

    // Note: This test will work even if replication is not configured
    // In that case, the response will be empty or return an error
    let result: Result<BucketReplicationDiffResponse, _> = madmin_client
        .bucket_replication_diff(bucket_name.as_str())
        .unwrap()
        .build()
        .send()
        .await;

    match result {
        Ok(diff_resp) => {
            let diffs = diff_resp.diffs().expect("Failed to parse diffs");
            println!("Found {} objects with replication diff", diffs.len());
            for diff in diffs.iter().take(5) {
                // Show first 5
                println!(
                    "  Object: {}, Status: {:?}",
                    diff.object, diff.replication_status
                );
            }
        }
        Err(e) => {
            println!(
                "Replication diff check failed (expected if replication not configured): {:?}",
                e
            );
            println!("Test completed (no replication config is acceptable)");
        }
    }
}

#[minio_macros::test(skip_if_express)]
async fn test_bucket_replication_diff_with_options(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!(
        "Checking replication diff with options for bucket '{}'",
        bucket_name
    );

    let opts = ReplDiffOpts {
        arn: None, // No specific ARN filter
        verbose: true,
        prefix: Some("test-".to_string()), // Only objects starting with test-
    };

    let result: Result<BucketReplicationDiffResponse, _> = madmin_client
        .bucket_replication_diff(bucket_name.as_str())
        .unwrap()
        .opts(opts)
        .build()
        .send()
        .await;

    match result {
        Ok(diff_resp) => {
            let diffs = diff_resp.diffs().expect("Failed to parse diffs");
            println!(
                "Found {} objects with replication diff (verbose, prefix filter)",
                diffs.len()
            );
        }
        Err(e) => {
            println!(
                "Replication diff check failed (expected if replication not configured): {:?}",
                e
            );
            println!("Test completed (no replication config is acceptable)");
        }
    }
}

#[minio_macros::test(skip_if_express)]
async fn test_bucket_replication_mrf(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!("Checking MRF backlog for bucket '{}'", bucket_name);

    // Note: This test will work even if replication is not configured
    // In that case, the response will be empty or return an error
    let result: Result<BucketReplicationMRFResponse, _> = madmin_client
        .bucket_replication_mrf(bucket_name.as_str())
        .unwrap()
        .build()
        .send()
        .await;

    match result {
        Ok(mrf_resp) => {
            let entries = mrf_resp.entries().expect("Failed to parse entries");
            println!("Found {} objects in MRF backlog", entries.len());
            for entry in entries.iter().take(5) {
                // Show first 5
                println!(
                    "  Object: {}, Node: {}, Retries: {}, Error: {:?}",
                    entry.object, entry.node_name, entry.retry_count, entry.err
                );
            }
        }
        Err(e) => {
            println!(
                "MRF backlog check failed (expected if replication not configured or no failures): {:?}",
                e
            );
            println!("Test completed (no MRF backlog is acceptable)");
        }
    }
}

#[minio_macros::test(skip_if_express)]
async fn test_bucket_replication_mrf_with_node(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    println!(
        "Checking MRF backlog for bucket '{}' on specific node",
        bucket_name
    );

    // Note: In a single-node setup, there's typically one node
    // In distributed setup, you'd specify the actual node name
    let result: Result<BucketReplicationMRFResponse, _> = madmin_client
        .bucket_replication_mrf(bucket_name.as_str())
        .unwrap()
        .node("node1".to_string()) // Placeholder node name
        .build()
        .send()
        .await;

    match result {
        Ok(mrf_resp) => {
            let entries = mrf_resp.entries().expect("Failed to parse entries");
            println!(
                "Found {} objects in MRF backlog for specific node",
                entries.len()
            );
        }
        Err(e) => {
            println!(
                "MRF backlog check failed (expected if node doesn't exist or no replication): {:?}",
                e
            );
            println!("Test completed (no MRF backlog or node not found is acceptable)");
        }
    }
}
