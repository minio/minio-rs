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

//! Example: MinIO Admin - Monitoring and Metrics
//!
//! Demonstrates monitoring operations including:
//! - Getting server information
//! - Retrieving storage usage
//! - Getting account information
//! - Checking data usage statistics

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize admin client with credentials
    let base_url: BaseUrl = std::env::var("MINIO_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string())
        .parse()?;

    let access_key = std::env::var("MINIO_ROOT_USER").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("MINIO_ROOT_PASSWORD").unwrap_or_else(|_| "minioadmin".to_string());

    let provider = StaticProvider::new(&access_key, &secret_key, None);
    let madmin_client = MadminClient::new(base_url, Some(provider));

    println!("=== MinIO Monitoring Example ===\n");

    // 1. Get server information
    println!("1. Getting server information...");
    let server_info = madmin_client.server_info().build().send().await?;

    println!(
        "   Deployment ID: {}",
        server_info.info().unwrap().deployment_id
    );
    println!("   Mode: {}", server_info.info().unwrap().mode);

    if let Some(servers) = &server_info.info().unwrap().servers {
        println!("   Number of servers: {}", servers.len());
        for server in servers {
            println!("   Server:");
            println!("     State: {}", server.state);
            println!("     Endpoint: {}", server.endpoint);
            println!("     Uptime: {} seconds", server.uptime);
        }
    }
    println!();

    // 2. Get storage information
    println!("2. Getting storage information...");
    let storage_info = madmin_client.storage_info().build().send().await?;

    println!(
        "   Storage Backend: {:?}",
        storage_info.backend.backend_type
    );
    if !storage_info.backend.standard_sc_data.is_empty() {
        println!(
            "   Standard storage class data shards: {:?}",
            storage_info.backend.standard_sc_data
        );
    }
    if !storage_info.backend.standard_sc_parities.is_empty() {
        println!(
            "   Standard storage class parity shards: {:?}",
            storage_info.backend.standard_sc_parities
        );
    }
    println!();

    // 3. Get account information
    println!("3. Getting account information...");
    let account_info = madmin_client.account_info().build().send().await?;

    println!(
        "   Account: {}",
        account_info.account().unwrap().account_name
    );
    println!(
        "   Number of buckets: {}",
        account_info.account().unwrap().buckets.len()
    );

    let mut total_size: u64 = 0;
    let mut total_objects: u64 = 0;

    for bucket in &account_info.account().unwrap().buckets {
        total_size += bucket.size;
        total_objects += bucket.objects;
        if bucket.objects > 0 || bucket.size > 0 {
            println!(
                "   Bucket '{}': {} objects, {} bytes",
                bucket.name, bucket.objects, bucket.size
            );
        }
    }

    println!(
        "\n   Total: {} objects, {} bytes",
        total_objects, total_size
    );
    println!();

    // 4. Get data usage information
    println!("4. Getting data usage information...");
    let data_usage = madmin_client.data_usage_info().build().send().await?;

    println!("   Total buckets: {}", data_usage.info.buckets_count);
    println!("   Total objects: {}", data_usage.info.objects_count);
    println!(
        "   Total size: {} bytes",
        data_usage.info.objects_total_size
    );

    if let Some(buckets_usage) = data_usage.info.buckets_usage {
        println!("\n   Per-bucket usage:");
        for (bucket_name, usage) in buckets_usage.iter().take(10) {
            println!("     {}: {} bytes", bucket_name, usage.size);
        }
    }
    println!();

    println!("=== Example completed successfully ===");
    Ok(())
}
