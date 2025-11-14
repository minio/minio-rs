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
use minio::madmin::response::{ServerInfoResponse, StorageInfoResponse};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_server_info() {
    let ctx = TestContext::new_from_env();

    // Create MadminClient with credentials
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Get server information
    let resp: ServerInfoResponse = madmin_client
        .server_info()
        .build()
        .send()
        .await
        .expect("Failed to get server info");

    let info = resp.info().expect("Failed to parse server info");

    // Validate basic response structure
    assert!(!info.mode.is_empty(), "Server mode should not be empty");
    assert!(
        !info.deployment_id.is_empty(),
        "Deployment ID should not be empty"
    );

    // Validate that we got at least one server in the response
    if let Some(servers) = &info.servers {
        assert!(!servers.is_empty(), "Should have at least one server");

        // Validate first server properties
        let server = &servers[0];
        assert!(
            !server.endpoint.is_empty(),
            "Server endpoint should not be empty"
        );
        assert!(!server.state.is_empty(), "Server state should not be empty");
        assert!(
            !server.version.is_empty(),
            "Server version should not be empty"
        );
        assert!(
            !server.commit_id.is_empty(),
            "Server commit ID should not be empty"
        );

        println!("✓ Server endpoint: {}", server.endpoint);
        println!("✓ Server state: {}", server.state);
        println!("✓ Server version: {}", server.version);
        println!("✓ Server uptime: {} seconds", server.uptime);
    }

    // Validate bucket count (if available)
    if let Some(buckets) = &info.buckets {
        println!("✓ Bucket count: {}", buckets.count);
    }

    // Validate object count (if available)
    if let Some(objects) = &info.objects {
        println!("✓ Object count: {}", objects.count);
    }

    // Validate usage information (if available)
    if let Some(usage) = &info.usage {
        println!("✓ Storage usage: {} bytes", usage.size);
    }

    // Validate backend information (if available)
    if let Some(backend) = &info.backend {
        if let Some(online_disks) = backend.online_disks {
            println!("✓ Online disks: {}", online_disks);
        }
        if let Some(offline_disks) = backend.offline_disks {
            println!("✓ Offline disks: {}", offline_disks);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_server_info_with_drive_details() {
    let ctx = TestContext::new_from_env();

    // Create MadminClient with credentials
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Get server information
    let resp: ServerInfoResponse = madmin_client
        .server_info()
        .build()
        .send()
        .await
        .expect("Failed to get server info");

    let info = resp.info().expect("Failed to parse server info");

    // Check if we have drive information
    if let Some(servers) = &info.servers {
        for (i, server) in servers.iter().enumerate() {
            println!("\nServer {}: {}", i + 1, server.endpoint);

            if let Some(drives) = &server.drives {
                println!("  Drives: {}", drives.len());
                assert!(!drives.is_empty(), "Server should have at least one drive");

                for (j, drive) in drives.iter().enumerate() {
                    println!("    Drive {}: {}", j + 1, drive.endpoint);
                    if let Some(uuid) = &drive.uuid {
                        println!("      UUID: {}", uuid);
                    }
                    println!("      State: {}", drive.state);
                    if let Some(root_disk) = drive.root_disk {
                        println!("      Root Disk: {}", root_disk);
                    }
                    println!("      Total: {} bytes", drive.totalspace);
                    println!("      Used: {} bytes", drive.usedspace);
                    println!("      Available: {} bytes", drive.availspace);
                    if let Some(util) = drive.utilization {
                        println!("      Utilization: {:.2}%", util * 100.0);
                    }

                    // Validate drive properties
                    if let Some(uuid) = &drive.uuid {
                        assert!(!uuid.is_empty(), "Drive UUID should not be empty");
                    }
                    assert!(
                        !drive.endpoint.is_empty(),
                        "Drive endpoint should not be empty"
                    );
                    assert!(!drive.state.is_empty(), "Drive state should not be empty");
                    assert!(
                        drive.usedspace <= drive.totalspace,
                        "Used space should not exceed total space"
                    );
                }
            }

            // Check memory stats if available
            if let Some(mem_stats) = &server.mem_stats {
                println!("  Memory:");
                println!("    Allocated: {} bytes", mem_stats.alloc);
                println!("    Total Allocated: {} bytes", mem_stats.total_alloc);
                println!("    Heap Allocated: {} bytes", mem_stats.heap_alloc);
            }

            // Check GC stats if available
            if let Some(gc_stats) = &server.gc_stats {
                println!("  GC:");
                println!("    Last GC: {}", gc_stats.last_gc);
                println!("    Num GC: {}", gc_stats.num_gc);
                if let Some(pause_total) = gc_stats.pause_total {
                    println!("    Pause Total: {} ns", pause_total);
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "StorageInfo struct definitions need verification against actual MinIO response format"]
async fn test_storage_info() {
    let ctx = TestContext::new_from_env();

    // Create MadminClient with credentials
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Get storage information
    let resp: StorageInfoResponse = madmin_client
        .storage_info()
        .build()
        .send()
        .await
        .expect("Failed to get storage info");

    // StorageInfoResponse derefs to StorageInfo
    // Validate disks
    if !resp.disks.is_empty() {
        println!("Total disks: {}", resp.disks.len());

        // Check first disk
        let disk = &resp.disks[0];
        assert!(
            !disk.endpoint.is_empty(),
            "Disk endpoint should not be empty"
        );
        println!("✓ Disk endpoint: {}", disk.endpoint);
        println!("✓ Disk state: {}", disk.state);
        println!("✓ Total space: {} bytes", disk.total_space);
        println!("✓ Used space: {} bytes", disk.used_space);
        println!("✓ Available space: {} bytes", disk.available_space);

        // Validate space calculations
        if disk.total_space > 0 {
            assert!(
                disk.used_space <= disk.total_space,
                "Used space should not exceed total space"
            );
        }
    }

    // Validate backend information
    println!("Backend type: {:?}", resp.backend.backend_type);
    println!("Online disks: {:?}", resp.backend.online_disks);
    println!("Offline disks: {:?}", resp.backend.offline_disks);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "GetAPILogs returns streaming data that requires careful handling"]
async fn test_get_api_logs() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Get API logs with basic filters
    use minio::madmin::types::api_logs::APILogOpts;

    let opts = APILogOpts {
        node: Some("all".to_string()),
        ..Default::default()
    };

    let resp = madmin_client
        .get_api_logs()
        .opts(opts)
        .build()
        .send()
        .await
        .expect("Failed to get API logs");

    println!("API logs response received: {} bytes", resp.data.len());

    // MessagePack format validation
    assert!(!resp.data.is_empty(), "Should receive API log data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Inspect requires object path and returns encrypted binary data"]
async fn test_inspect() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Test bucket requires a pre-existing bucket with objects
    let test_bucket = std::env::var("TEST_BUCKET").unwrap_or_else(|_| "test-bucket".to_string());
    let test_object =
        std::env::var("TEST_OBJECT").unwrap_or_else(|_| "test-object.txt".to_string());

    // Inspect object metadata
    use minio::madmin::types::inspect::InspectOptions;

    let opts = InspectOptions {
        volume: Some(test_bucket.clone()),
        file: Some(test_object.clone()),
        ..Default::default()
    };

    let resp = madmin_client
        .inspect()
        .opts(opts)
        .build()
        .send()
        .await
        .expect("Failed to inspect object");

    println!("Inspect response received: {} bytes", resp.data.data.len());
    println!("Format: {:?}", resp.data.format);

    // Validate response
    assert!(!resp.data.data.is_empty(), "Should receive inspection data");

    if let Some(ref key) = resp.data.encryption_key {
        println!("Encryption key length: {} bytes", key.len());
        assert!(!key.is_empty(), "Encryption key should not be empty");
    }
}
