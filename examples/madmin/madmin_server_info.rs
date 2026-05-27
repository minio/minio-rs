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

//! Example demonstrating MinIO Admin API usage
//!
//! This example shows how to use the MinIO Admin (madmin) client to retrieve
//! server information from a MinIO cluster.
//!
//! # Usage
//!
//! Set environment variables:
//! - `MINIO_ENDPOINT`: MinIO server endpoint (default: localhost:9000)
//! - `MINIO_ACCESS_KEY`: Access key (default: minioadmin)
//! - `MINIO_SECRET_KEY`: Secret key (default: minioadmin)
//!
//! Run the example:
//! ```bash
//! cargo run --example madmin_server_info
//! ```

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();

    // Read configuration from environment
    let endpoint = env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "localhost:9000".to_string());
    let access_key = env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());

    println!("=== MinIO Admin API Example ===");
    println!("Endpoint: {}", endpoint);
    println!();

    // Parse base URL
    let base_url = endpoint.parse::<BaseUrl>()?;

    // Create credentials provider
    let provider = StaticProvider::new(&access_key, &secret_key, None);

    // Create madmin client
    let client = MadminClient::new(base_url, Some(provider));

    // Get server information
    println!("Fetching server information...");
    let response = client.server_info().build().send().await?;

    // Display server information
    println!("\n=== Server Information ===");
    println!("Mode: {}", response.info().unwrap().mode);
    println!("Deployment ID: {}", response.info().unwrap().deployment_id);

    // Display bucket information
    if let Some(buckets) = &response.info().unwrap().buckets {
        println!("\nBuckets:");
        println!("  Count: {}", buckets.count);
        if let Some(error) = &buckets.error {
            println!("  Error: {}", error);
        }
    }

    // Display object information
    if let Some(objects) = &response.info().unwrap().objects {
        println!("\nObjects:");
        println!("  Count: {}", objects.count);
        if let Some(error) = &objects.error {
            println!("  Error: {}", error);
        }
    }

    // Display usage information
    if let Some(usage) = &response.info().unwrap().usage {
        println!("\nUsage:");
        println!("  Size: {} bytes", usage.size);
        if let Some(error) = &usage.error {
            println!("  Error: {}", error);
        }
    }

    // Display backend information
    if let Some(backend) = &response.info().unwrap().backend {
        println!("\nBackend:");
        if let Some(backend_type) = &backend.backend_type {
            println!("  Type: {}", backend_type.backend_type);
        }
        if let Some(online) = backend.online_disks {
            println!("  Online Disks: {}", online);
        }
        if let Some(offline) = backend.offline_disks {
            println!("  Offline Disks: {}", offline);
        }
    }

    // Display server details
    if let Some(servers) = &response.info().unwrap().servers {
        println!("\nServers ({}):", servers.len());
        for (i, server) in servers.iter().enumerate() {
            println!("\n  Server {}:", i + 1);
            println!("    Endpoint: {}", server.endpoint);
            println!("    State: {}", server.state);
            println!("    Version: {}", server.version);
            println!("    Commit ID: {}", server.commit_id);
            println!("    Uptime: {} seconds", server.uptime);

            if let Some(drives) = &server.drives {
                println!("    Drives: {}", drives.len());
                for drive in drives {
                    println!("      - {} ({})", drive.endpoint, drive.state);
                    println!("        Total: {} bytes", drive.totalspace);
                    println!("        Used: {} bytes", drive.usedspace);
                    println!("        Available: {} bytes", drive.availspace);
                    if let Some(utilization) = drive.utilization {
                        println!("        Utilization: {:.2}%", utilization * 100.0);
                    }
                }
            }

            if let Some(mem_stats) = &server.mem_stats {
                println!("    Memory:");
                println!("      Allocated: {} bytes", mem_stats.alloc);
                println!("      Total Allocated: {} bytes", mem_stats.total_alloc);
                println!("      Heap Allocated: {} bytes", mem_stats.heap_alloc);
            }
        }
    }

    println!("\n=== Success ===");
    Ok(())
}
