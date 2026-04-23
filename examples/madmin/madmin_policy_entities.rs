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

//! Example: MinIO Admin - Policy Entities
//!
//! Demonstrates getting policy entity associations including:
//! - Query entities by policy name
//! - Query entities by user
//! - Query entities by group
//! - View user-policy mappings
//! - View group-policy mappings
//! - View policy-entity mappings

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::MadminApi;
use minio::madmin::types::policy::PolicyEntitiesQuery;
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

    println!("=== MinIO Policy Entities Example ===\n");

    // 1. Query all policy entities (no filters)
    println!("1. Getting all policy entities...");
    let query = PolicyEntitiesQuery::default();

    let response = madmin_client
        .get_policy_entities()
        .query(query)
        .build()
        .send()
        .await?;

    println!("   Timestamp: {}", response.entities().timestamp);

    if let Some(user_mappings) = &response.entities().user_mappings {
        println!("   Found {} user-policy mappings", user_mappings.len());
        for mapping in user_mappings.iter().take(3) {
            println!(
                "     User: {} -> Policies: {:?}",
                mapping.user, mapping.policies
            );
        }
    }

    if let Some(group_mappings) = &response.entities().group_mappings {
        println!("   Found {} group-policy mappings", group_mappings.len());
        for mapping in group_mappings.iter().take(3) {
            println!(
                "     Group: {} -> Policies: {:?}",
                mapping.group, mapping.policies
            );
        }
    }

    if let Some(policy_mappings) = &response.entities().policy_mappings {
        println!("   Found {} policy-entity mappings", policy_mappings.len());
        for mapping in policy_mappings.iter().take(3) {
            println!(
                "     Policy: {} -> Users: {:?}, Groups: {:?}",
                mapping.policy, mapping.users, mapping.groups
            );
        }
    }
    println!();

    // 2. Query entities for a specific policy
    println!("2. Getting entities for 'readwrite' policy...");
    let query = PolicyEntitiesQuery {
        users: vec![],
        groups: vec![],
        policy: vec!["readwrite".to_string()],
        config_name: None,
    };

    let response = madmin_client
        .get_policy_entities()
        .query(query)
        .build()
        .send()
        .await?;

    if let Some(policy_mappings) = &response.entities().policy_mappings {
        for mapping in policy_mappings {
            println!("   Policy '{}' is attached to:", mapping.policy);
            println!("     Users: {:?}", mapping.users);
            println!("     Groups: {:?}", mapping.groups);
        }
    } else {
        println!("   No entities found for 'readwrite' policy");
    }
    println!();

    // 3. Query policies for a specific user (if exists)
    println!("3. Attempting to query policies for a specific user...");
    let query = PolicyEntitiesQuery {
        users: vec!["example-user".to_string()],
        groups: vec![],
        policy: vec![],
        config_name: None,
    };

    match madmin_client
        .get_policy_entities()
        .query(query)
        .build()
        .send()
        .await
    {
        Ok(response) => {
            if let Some(user_mappings) = &response.entities().user_mappings {
                for mapping in user_mappings {
                    println!(
                        "   User '{}' has policies: {:?}",
                        mapping.user, mapping.policies
                    );
                    if let Some(member_of) = &mapping.member_of_mappings {
                        println!("   Member of groups:");
                        for group in member_of {
                            println!(
                                "     - Group: {} (Policies: {:?})",
                                group.group, group.policies
                            );
                        }
                    }
                }
            } else {
                println!("   User 'example-user' not found");
            }
        }
        Err(e) => {
            println!("   User 'example-user' not found or error: {}", e);
        }
    }
    println!();

    println!("=== Example completed successfully ===");
    Ok(())
}
