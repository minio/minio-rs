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

//! Example: MinIO Admin - Configuration History Management
//!
//! Demonstrates configuration history operations including:
//! - Listing configuration history entries
//! - Viewing configuration snapshots with restore IDs
//! - Restoring previous configurations
//! - Clearing history entries

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

    println!("=== MinIO Configuration History Management Example ===\n");

    // 1. List configuration history (last 10 entries by default)
    println!("1. Listing configuration history...");
    let response = madmin_client
        .list_config_history_kv()
        .count(10u32)
        .build()
        .send()
        .await?;

    let entries = response.entries().expect("Failed to get entries");
    println!("   Found {} configuration history entries:", entries.len());

    let mut restore_id_to_use = None;
    for (idx, entry) in entries.iter().enumerate() {
        println!("   [{}] Restore ID: {}", idx + 1, entry.restore_id);
        println!("       Created: {}", entry.create_time);
        println!("       Data length: {} bytes", entry.data.len());

        // Save the first restore ID for demonstration
        if idx == 0 && restore_id_to_use.is_none() {
            restore_id_to_use = Some(entry.restore_id.clone());
        }
    }
    println!();

    // 2. Demonstrate restore capability (commented out for safety)
    if let Some(restore_id) = restore_id_to_use {
        println!(
            "2. Example: Restoring configuration with ID '{}'",
            restore_id
        );
        println!("   (Skipped in example - uncomment to actually restore)");

        // Uncomment the following to actually restore a configuration:
        /*
        madmin_client
            .restore_config_history_kv()
            .restore_id(restore_id.clone())
            .build()
            .send()
            .await?;
        println!("   Configuration restored successfully!");
        */
        println!();

        // 3. Demonstrate clearing a specific history entry (commented out for safety)
        println!("3. Example: Clearing specific history entry");
        println!("   (Skipped in example - uncomment to actually clear)");

        // Uncomment the following to actually clear a history entry:
        /*
        madmin_client
            .clear_config_history_kv()
            .restore_id(restore_id)
            .build()
            .send()
            .await?;
        println!("   History entry cleared successfully!");
        */
        println!();
    }

    // 4. Demonstrate clearing all history (commented out for safety)
    println!("4. Example: Clearing all configuration history");
    println!("   (Skipped in example - uncomment to actually clear all history)");

    // Uncomment the following to actually clear all history:
    /*
    madmin_client
        .clear_config_history_kv()
        .restore_id("all")
        .build()
        .send()
        .await?;
    println!("   All history cleared successfully!");
    */
    println!();

    // 5. Show typical workflow
    println!("5. Typical Configuration History Workflow:");
    println!("   a) Make configuration changes using SetConfig or SetConfigKV");
    println!("   b) MinIO automatically saves a history entry with a restore ID");
    println!("   c) List history to find the restore ID you want");
    println!("   d) Use RestoreConfigHistoryKV to revert to a previous state");
    println!("   e) Optionally clear old history entries to save space");
    println!();

    println!("=== Example completed successfully ===");
    println!("Note: Destructive operations (restore, clear) are commented out for safety.");
    println!("Uncomment them in the source code to test actual restoration and clearing.");

    Ok(())
}
