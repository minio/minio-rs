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

//! Example: MinIO Admin - Policy Management
//!
//! Demonstrates IAM policy operations including:
//! - Creating policies
//! - Listing policies
//! - Attaching policies to users
//! - Detaching policies from users
//! - Removing policies

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::MadminApi;
use minio::madmin::types::policy::PolicyAssociationReq;
use minio::madmin::types::typed_parameters::PolicyName;
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

    println!("=== MinIO Policy Management Example ===\n");

    // 1. List existing policies
    println!("1. Listing existing policies...");
    let policies = madmin_client.list_canned_policies().build().send().await?;
    println!("   Found {} policies:", policies.policies().unwrap().len());
    for name in policies.policies().unwrap().keys() {
        println!("   - {}", name);
    }
    println!();

    // 2. Create a custom policy
    let policy_name = PolicyName::new("example-readonly-policy")?;
    println!("2. Creating custom policy '{}'...", policy_name);

    let policy_doc = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::example-*",
                "arn:aws:s3:::example-*/*"
            ]
        }]
    });

    let policy_bytes = serde_json::to_vec(&policy_doc)?;

    madmin_client
        .add_canned_policy()
        .policy_name(&policy_name)
        .policy(policy_bytes)
        .build()
        .send()
        .await?;

    println!("   Policy created successfully\n");

    // 3. Get policy information
    println!("3. Getting policy information...");
    let policy_info = madmin_client
        .info_canned_policy()
        .policy_name(&policy_name)
        .build()
        .send()
        .await?;

    let policy_data = policy_info.info()?;
    println!("   Policy retrieved successfully");
    if !policy_data.policy_name.is_empty() {
        println!("   Policy Name: {}", policy_data.policy_name);
    }
    println!();

    // 4. Create a test user to attach the policy to
    let test_user = "example-policy-user";
    println!("4. Creating test user '{}'...", test_user);

    madmin_client
        .add_user(test_user, "TestPassword123!")?
        .build()
        .send()
        .await?;

    println!("   User created\n");

    // 5. Attach policy to user
    println!("5. Attaching policy to user...");

    let attach_req = PolicyAssociationReq {
        policies: vec![policy_name.to_string()],
        user: Some(test_user.to_string()),
        group: None,
        config_name: None,
    };

    let attach_resp = madmin_client
        .attach_policy()
        .request(attach_req)
        .build()
        .send()
        .await?;

    if let Some(attached) = &attach_resp.policies_attached {
        println!("   Attached {} policies", attached.len());
    }
    println!();

    // 6. Detach policy from user
    println!("6. Detaching policy from user...");

    let detach_req = PolicyAssociationReq {
        policies: vec![policy_name.to_string()],
        user: Some(test_user.to_string()),
        group: None,
        config_name: None,
    };

    let detach_resp = madmin_client
        .detach_policy()
        .request(detach_req)
        .build()
        .send()
        .await?;

    if let Some(detached) = &detach_resp.policies_detached {
        println!("   Detached {} policies", detached.len());
    }
    println!();

    // 7. Clean up: remove user and policy
    println!("7. Cleaning up...");

    madmin_client
        .remove_user(test_user)?
        .build()
        .send()
        .await?;
    println!("   User removed");

    madmin_client
        .remove_canned_policy()
        .policy_name(&policy_name)
        .build()
        .send()
        .await?;
    println!("   Policy removed\n");

    println!("=== Example completed successfully ===");
    Ok(())
}
