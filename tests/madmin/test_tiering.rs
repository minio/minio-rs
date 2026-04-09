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

//! Integration tests for Tiering APIs
//!
//! Tests the following APIs:
//! - AddTier (add remote storage tier)
//! - ListTiers (list all tiers)
//! - EditTier (modify tier credentials)
//! - RemoveTier (remove tier)
//! - VerifyTier (verify tier connectivity)
//! - TierStats (get tier usage statistics)

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{
    AddTierResponse, EditTierResponse, ListTiersResponse, RemoveTierResponse, TierStatsResponse,
    VerifyTierResponse,
};
use minio::madmin::types::MadminApi;
use minio::madmin::types::tier::{TIER_CONFIG_VER, TierConfig, TierS3, TierType};
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

fn get_madmin_client() -> MadminClient {
    let ctx = TestContext::new_from_env();
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    MadminClient::new(ctx.base_url.clone(), Some(provider))
}

fn create_test_tier_config(tier_name: &str) -> TierConfig {
    TierConfig {
        version: TIER_CONFIG_VER.to_string(),
        tier_type: Some(TierType::S3),
        name: Some(tier_name.to_string()),
        s3: Some(TierS3 {
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            access_key: Some("test-access-key".to_string()),
            secret_key: Some("test-secret-key".to_string()),
            bucket: Some("test-tier-bucket".to_string()),
            prefix: Some("tier-data/".to_string()),
            region: Some("us-east-1".to_string()),
            storage_class: Some("STANDARD".to_string()),
        }),
        azure: None,
        gcs: None,
        minio: None,
    }
}

#[tokio::test]
async fn test_list_tiers() {
    let madmin = get_madmin_client();

    let tiers_resp: ListTiersResponse = madmin
        .list_tiers()
        .send()
        .await
        .expect("Failed to list tiers");

    let tiers = tiers_resp.tiers().unwrap();
    println!("Found {} tier(s)", tiers.len());

    for tier in &tiers {
        println!("Tier: {:?}", tier.name);
        println!("  Type: {:?}", tier.tier_type);
        println!("  Version: {}", tier.version);

        if tier.s3.is_some() {
            println!("  Backend: S3");
        } else if tier.azure.is_some() {
            println!("  Backend: Azure");
        } else if tier.gcs.is_some() {
            println!("  Backend: GCS");
        } else if tier.minio.is_some() {
            println!("  Backend: MinIO");
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_add_and_remove_tier() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Requires valid cloud storage credentials
    // 2. Creates actual remote tier configurations
    // 3. May incur cloud storage costs
    //
    // To run this test:
    // - Configure valid S3/Azure/GCS credentials
    // - Remove the skip attribute
    // - Run: cargo test test_add_and_remove_tier -- --nocapture

    let tier_name = format!("test-tier-{}", chrono::Utc::now().timestamp());
    let config = create_test_tier_config(&tier_name);

    println!("Adding tier: {}", tier_name);

    // Add tier (with default force=false)
    let _add: AddTierResponse = madmin
        .add_tier(config)
        .send()
        .await
        .expect("Failed to add tier");

    println!("Tier added successfully");

    // List tiers to verify
    let tiers_resp: ListTiersResponse = madmin
        .list_tiers()
        .send()
        .await
        .expect("Failed to list tiers");

    let tiers = tiers_resp.tiers().unwrap();
    let found = tiers.iter().any(|t| t.name.as_ref() == Some(&tier_name));
    assert!(found, "Newly added tier should be in list");

    // Remove tier
    println!("Removing tier: {}", tier_name);
    let _remove: RemoveTierResponse = madmin
        .remove_tier(&tier_name)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove tier");

    println!("Tier removed successfully");

    // Verify removal
    let tiers_resp: ListTiersResponse = madmin
        .list_tiers()
        .send()
        .await
        .expect("Failed to list tiers");

    let tiers = tiers_resp.tiers().unwrap();
    let not_found = !tiers.iter().any(|t| t.name.as_ref() == Some(&tier_name));
    assert!(not_found, "Removed tier should not be in list");
}

#[tokio::test]
#[ignore]
async fn test_verify_tier() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Requires an existing tier to be configured
    // 2. Makes actual network calls to verify connectivity
    //
    // To run this test:
    // - Ensure you have at least one tier configured
    // - Remove the skip attribute
    // - Run: cargo test test_verify_tier -- --nocapture

    // Get list of tiers
    let tiers_resp: ListTiersResponse = madmin
        .list_tiers()
        .send()
        .await
        .expect("Failed to list tiers");

    let tiers = tiers_resp.tiers().unwrap();
    if tiers.is_empty() {
        println!("No tiers configured, skipping verify test");
        return;
    }

    let tier_name = tiers[0].name.as_ref().expect("Tier should have a name");

    println!("Verifying tier: {}", tier_name);

    let _verify: VerifyTierResponse = madmin
        .verify_tier(tier_name)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to verify tier");

    println!("Tier verified successfully");
}

#[tokio::test]
#[ignore]
async fn test_tier_stats() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Requires an existing tier with data
    // 2. Stats may take time to compute
    //
    // To run this test:
    // - Ensure you have tiers with data
    // - Remove the skip attribute
    // - Run: cargo test test_tier_stats -- --nocapture

    let stats_resp: TierStatsResponse = madmin
        .tier_stats()
        .send()
        .await
        .expect("Failed to get tier stats");

    let stats = stats_resp.stats().unwrap();
    println!("Tier Statistics:");
    println!("  Found {} tier(s) with stats", stats.len());

    for stat in &stats {
        println!("  Tier: {}", stat.name);
        println!("    Type: {}", stat.tier_type);
        println!("    Objects: {}", stat.stats.num_objects);
        println!("    Versions: {}", stat.stats.num_versions);
        println!("    Total Size: {} bytes", stat.stats.total_size);
    }
}

#[tokio::test]
#[ignore]
async fn test_edit_tier_credentials() {
    let madmin = get_madmin_client();
    // NOTE: This test is skipped by default because:
    // 1. Requires an existing tier
    // 2. Modifies tier credentials
    // 3. Requires valid new credentials
    //
    // To run this test:
    // - Ensure you have a tier configured
    // - Provide valid new credentials
    // - Remove the skip attribute
    // - Run: cargo test test_edit_tier_credentials -- --nocapture

    let tiers_resp: ListTiersResponse = madmin
        .list_tiers()
        .send()
        .await
        .expect("Failed to list tiers");

    let tiers = tiers_resp.tiers().unwrap();
    if tiers.is_empty() {
        println!("No tiers configured, skipping edit test");
        return;
    }

    let tier_name = tiers[0].name.as_ref().expect("Tier should have a name");

    println!("Editing credentials for tier: {}", tier_name);

    // Create new credentials (use test credentials)
    let new_creds = minio::madmin::types::tier::TierCreds {
        access_key: Some("new-access-key".to_string()),
        secret_key: Some("new-secret-key".to_string()),
        creds_json: None,
    };

    let _edit: EditTierResponse = madmin
        .edit_tier(tier_name, new_creds)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to edit tier");

    println!("Tier credentials updated successfully");
}

#[tokio::test]
async fn test_tier_type_serialization() {
    let _madmin = get_madmin_client();
    use serde_json;

    // Test TierType serialization
    let tier_type = TierType::S3;
    let json = serde_json::to_string(&tier_type).unwrap();
    assert_eq!(json, "\"s3\"");

    let tier_type = TierType::Azure;
    let json = serde_json::to_string(&tier_type).unwrap();
    assert_eq!(json, "\"azure\"");

    let tier_type = TierType::GCS;
    let json = serde_json::to_string(&tier_type).unwrap();
    assert_eq!(json, "\"gcs\"");

    let tier_type = TierType::MinIO;
    let json = serde_json::to_string(&tier_type).unwrap();
    assert_eq!(json, "\"minio\"");
}
