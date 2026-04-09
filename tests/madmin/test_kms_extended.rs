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
use minio::madmin::response::{
    KMSAPIsResponse, KMSCreateKeyResponse, KMSDeleteKeyResponse, KMSDeletePolicyResponse,
    KMSDescribePolicyResponse, KMSDescribeSelfIdentityResponse, KMSGetKeyStatusResponse,
    KMSGetPolicyResponse, KMSImportKeyResponse, KMSListIdentitiesResponse, KMSListKeysResponse,
    KMSListPoliciesResponse, KMSMetricsResponse, KMSSetPolicyResponse, KMSVersionResponse,
};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES (Key Encryption Service) server"]
async fn test_kms_metrics() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: KMSMetricsResponse = madmin_client
        .kms_metrics()
        .build()
        .send()
        .await
        .expect("Failed to get KMS metrics");

    println!("KMS Metrics received");
    println!("✓ KMSMetrics API call successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES server"]
async fn test_kms_apis() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: KMSAPIsResponse = madmin_client
        .kms_apis()
        .build()
        .send()
        .await
        .expect("Failed to get KMS APIs");

    println!("KMS APIs: {} entries", resp.len());
    println!("✓ KMSAPIs API call successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES server"]
async fn test_kms_version() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: KMSVersionResponse = madmin_client
        .kms_version()
        .build()
        .send()
        .await
        .expect("Failed to get KMS version");

    println!("KMS Version received");
    println!("✓ KMSVersion API call successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES server"]
async fn test_kms_key_lifecycle() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let key_id = format!("test-key-{}", uuid::Uuid::new_v4());

    // Create key
    println!("Creating KMS key: {}", key_id);
    let _create: KMSCreateKeyResponse = madmin_client
        .kms_create_key()
        .key_id(&key_id)
        .build()
        .send()
        .await
        .expect("Failed to create KMS key");

    println!("✓ Key created");

    // Get key status
    println!("Getting key status");
    let _status: KMSGetKeyStatusResponse = madmin_client
        .kms_get_key_status()
        .key_id(&key_id)
        .build()
        .send()
        .await
        .expect("Failed to get key status");

    println!("✓ Key status retrieved");

    // List keys
    println!("Listing keys");
    let keys: KMSListKeysResponse = madmin_client
        .kms_list_keys()
        .build()
        .send()
        .await
        .expect("Failed to list keys");

    println!("✓ Listed {} keys", keys.len());

    // Delete key
    println!("Deleting key");
    let _delete: KMSDeleteKeyResponse = madmin_client
        .kms_delete_key()
        .key_id(&key_id)
        .build()
        .send()
        .await
        .expect("Failed to delete KMS key");

    println!("✓ Key deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES server"]
async fn test_import_key() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let key_id = format!("imported-key-{}", uuid::Uuid::new_v4());
    let key_bytes = vec![0u8; 32]; // 256-bit key

    println!("Importing KMS key: {}", key_id);
    let _import: KMSImportKeyResponse = madmin_client
        .kms_import_key()
        .key_id(&key_id)
        .bytes(key_bytes)
        .build()
        .send()
        .await
        .expect("Failed to import KMS key");

    println!("✓ Key imported successfully");

    // Cleanup
    let _cleanup: Result<KMSDeleteKeyResponse, _> = madmin_client
        .kms_delete_key()
        .key_id(key_id)
        .build()
        .send()
        .await;
    let _ = _cleanup.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES server"]
async fn test_kms_policy_lifecycle() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let policy_name = format!("test-policy-{}", uuid::Uuid::new_v4());

    // Set KMS policy
    println!("Setting KMS policy: {}", policy_name);
    let policy_data = r#"{"allow":["kes:*"]}"#;

    let _set: KMSSetPolicyResponse = madmin_client
        .kms_set_policy()
        .policy(policy_name.clone())
        .policy_content(policy_data.to_string())
        .build()
        .send()
        .await
        .expect("Failed to set KMS policy");

    println!("✓ Policy set");

    // List policies
    println!("Listing policies");
    let policies: KMSListPoliciesResponse = madmin_client
        .kms_list_policies()
        .build()
        .send()
        .await
        .expect("Failed to list policies");

    println!("✓ Listed {} policies", policies.len());

    // Get policy
    println!("Getting policy");
    let _policy: KMSGetPolicyResponse = madmin_client
        .kms_get_policy()
        .policy(policy_name.clone())
        .build()
        .send()
        .await
        .expect("Failed to get policy");

    println!("✓ Policy retrieved");

    // Describe policy
    println!("Describing policy");
    let _desc: KMSDescribePolicyResponse = madmin_client
        .kms_describe_policy()
        .policy(policy_name.clone())
        .build()
        .send()
        .await
        .expect("Failed to describe policy");

    println!("✓ Policy described");

    // Delete policy
    println!("Deleting policy");
    let _del: KMSDeletePolicyResponse = madmin_client
        .kms_delete_policy()
        .policy(policy_name)
        .build()
        .send()
        .await
        .expect("Failed to delete policy");

    println!("✓ Policy deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "Requires external KES server"]
async fn test_kms_identity_operations() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // List identities
    println!("Listing identities");
    let identities: KMSListIdentitiesResponse = madmin_client
        .kms_list_identities()
        .build()
        .send()
        .await
        .expect("Failed to list identities");

    println!("  Total identities: {}", identities.len());
    println!("✓ Identities listed");

    // Describe self identity
    println!("Describing self identity");
    let _self_desc: KMSDescribeSelfIdentityResponse = madmin_client
        .kms_describe_self_identity()
        .build()
        .send()
        .await
        .expect("Failed to describe self identity");

    println!("✓ Self identity described");
}
