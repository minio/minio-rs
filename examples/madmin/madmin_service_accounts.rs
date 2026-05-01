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

//! Example: MinIO Admin - Service Account Management
//!
//! Demonstrates service account operations including:
//! - Creating service accounts
//! - Listing service accounts
//! - Getting service account information
//! - Updating service account policies
//! - Deleting service accounts

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::MadminApi;
use minio::madmin::types::service_account::{AddServiceAccountReq, UpdateServiceAccountReq};
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use serde_json::json;

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

    println!("=== MinIO Service Account Management Example ===\n");

    // 1. List existing service accounts
    println!("1. Listing existing service accounts...");
    let accounts_resp = madmin_client.list_service_accounts().build().send().await?;
    let accounts = accounts_resp.accounts()?;
    println!("   Found {} service accounts", accounts.len());
    for account in &accounts {
        println!("   - {:?}", account);
    }
    println!();

    // 2. Create a new service account with custom policy
    println!("2. Creating service account with custom policy...");

    // Define a read-only policy for a specific bucket
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                "arn:aws:s3:::example-bucket",
                "arn:aws:s3:::example-bucket/*"
            ]
        }]
    });

    let req = AddServiceAccountReq {
        policy: Some(policy),
        access_key: None,
        secret_key: None,
        name: Some("Example Service Account".to_string()),
        description: Some("Service account for read-only access to example-bucket".to_string()),
        expiration: None,
        target_user: None,
    };

    let new_account = madmin_client
        .add_service_account()
        .request(req)
        .build()
        .send()
        .await?;

    let credentials = new_account.credentials()?;
    println!("   Service account created:");
    println!("   Access Key: {}", credentials.access_key);
    println!("   Secret Key: {}", credentials.secret_key);
    println!();

    // Store the access key for later operations
    let service_access_key = credentials.access_key.clone();

    // 3. Get service account information
    println!("3. Getting service account information...");
    let account_info_resp = madmin_client
        .info_service_account(&service_access_key)?
        .build()
        .send()
        .await?;

    let account_info = account_info_resp.info()?;
    println!("   Name: {}", account_info.name.as_deref().unwrap_or("N/A"));
    println!(
        "   Description: {}",
        account_info.description.as_deref().unwrap_or("N/A")
    );
    println!("   Status: {}", account_info.account_status);
    println!();

    // 4. Update service account policy
    println!("4. Updating service account policy...");

    // Create a new policy with write access
    let updated_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [
                "arn:aws:s3:::example-bucket",
                "arn:aws:s3:::example-bucket/*"
            ]
        }]
    });

    let update_req = UpdateServiceAccountReq {
        new_policy: Some(updated_policy),
        new_secret_key: None,
        new_status: None,
        new_name: None,
        new_description: Some("Updated with full access to example-bucket".to_string()),
        new_expiration: None,
    };

    madmin_client
        .update_service_account()
        .access_key(&service_access_key)
        .request(update_req)
        .build()
        .send()
        .await?;

    println!("   Service account policy updated\n");

    // 5. Delete the service account
    println!("5. Deleting service account...");
    madmin_client
        .delete_service_account(&service_access_key)?
        .build()
        .send()
        .await?;
    println!("   Service account deleted successfully\n");

    println!("=== Example completed successfully ===");
    Ok(())
}
