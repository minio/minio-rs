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

//! Example: MinIO Admin - User Management
//!
//! Demonstrates user management operations including:
//! - Creating users
//! - Listing users
//! - Getting user information
//! - Enabling/disabling users
//! - Removing users

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::typed_parameters::AccessKey;
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

    println!("=== MinIO User Management Example ===\n");

    // 1. List existing users
    println!("1. Listing existing users...");
    let users_resp = madmin_client.list_users().build().send().await?;
    let users = users_resp.users()?;
    println!("   Found {} users:", users.len());
    for (username, user) in &users {
        println!("   - {}: status={}", username, user.status);
    }
    println!();

    // 2. Create a new user
    let new_username = AccessKey::new("example-user")?;

    println!("2. Creating user '{}'...", new_username);
    madmin_client
        .add_user(&new_username, "ExamplePassword123!")?
        .build()
        .send()
        .await?;
    println!("   User created successfully\n");

    // 3. Get user information
    println!("3. Getting user information...");
    let user_info_resp = madmin_client
        .user_info()
        .access_key(&new_username)
        .build()
        .send()
        .await?;
    let user_info = user_info_resp.user_info()?;
    println!("   Status: {}", user_info.status);
    println!("   Policies: {:?}", user_info.policy_name);
    println!();

    // 4. Disable the user
    println!("4. Disabling user '{}'...", new_username);
    madmin_client
        .set_user_status()
        .access_key(&new_username)
        .status("disabled".to_string())
        .build()
        .send()
        .await?;
    println!("   User disabled\n");

    // 5. Enable the user again
    println!("5. Re-enabling user '{}'...", new_username);
    madmin_client
        .set_user_status()
        .access_key(&new_username)
        .status("enabled".to_string())
        .build()
        .send()
        .await?;
    println!("   User enabled\n");

    // 6. Remove the user
    println!("6. Removing user '{}'...", new_username);
    madmin_client
        .remove_user(&new_username)?
        .build()
        .send()
        .await?;
    println!("   User removed successfully\n");

    println!("=== Example completed successfully ===");
    Ok(())
}
