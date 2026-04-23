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
use minio::madmin::response::{BackgroundHealStatusResponse, HealResponse, HealResult};
use minio::madmin::types::MadminApi;
use minio::madmin::types::heal::{HealOpts, HealScanMode};
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::types::{BucketName, S3Api};
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Heal APIs require distributed MinIO deployment (not available in single-node 'xl-single' mode)
async fn test_heal_start_dry_run() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider.clone()));

    let test_bucket_str = format!("test-heal-{}", chrono::Utc::now().timestamp());
    let test_bucket = BucketName::try_from(test_bucket_str.as_str()).unwrap();

    let s3_provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let s3_client = MinioClient::new(ctx.base_url.clone(), Some(s3_provider), None, None).unwrap();

    s3_client
        .create_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create test bucket");

    let opts = HealOpts {
        recursive: Some(true),
        dry_run: Some(true),
        scan_mode: Some(HealScanMode::Normal),
        ..Default::default()
    };

    let resp: HealResponse = madmin_client
        .heal()
        .bucket(Some(test_bucket.clone()))
        .opts(opts)
        .force_start(true)
        .build()
        .send()
        .await
        .expect("Failed to start heal operation");

    let result = resp.result().unwrap();
    match result {
        HealResult::Start(start_info) => {
            println!("✓ Heal started successfully");
            println!("  Client token: {}", start_info.client_token);
            println!("  Client address: {}", start_info.client_address);
            println!("  Start time: {}", start_info.start_time);

            assert!(
                !start_info.client_token.is_empty(),
                "Client token should not be empty"
            );
        }
        HealResult::Status(_) => {
            panic!("Expected HealResult::Start, got HealResult::Status");
        }
    }

    s3_client
        .delete_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove test bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Heal APIs require distributed MinIO deployment (not available in single-node 'xl-single' mode)
async fn test_heal_with_prefix() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider.clone()));

    let test_bucket_str = format!("test-heal-prefix-{}", chrono::Utc::now().timestamp());
    let test_bucket = BucketName::try_from(test_bucket_str.as_str()).unwrap();

    let s3_provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let s3_client = MinioClient::new(ctx.base_url.clone(), Some(s3_provider), None, None).unwrap();

    s3_client
        .create_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create test bucket");

    let opts = HealOpts {
        recursive: Some(false),
        dry_run: Some(true),
        scan_mode: Some(HealScanMode::Normal),
        ..Default::default()
    };

    let resp: HealResponse = madmin_client
        .heal()
        .bucket(Some(test_bucket.clone()))
        .prefix("test-prefix/".to_string())
        .opts(opts)
        .force_start(true)
        .build()
        .send()
        .await
        .expect("Failed to start heal operation with prefix");

    let result = resp.result().unwrap();
    match result {
        HealResult::Start(start_info) => {
            println!("✓ Heal with prefix started successfully");
            assert!(!start_info.client_token.is_empty());
        }
        HealResult::Status(_) => {
            panic!("Expected HealResult::Start, got HealResult::Status");
        }
    }

    s3_client
        .delete_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove test bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_heal_invalid_bucket() {
    // Test that BucketName validation rejects invalid bucket names
    let invalid_bucket = BucketName::try_from("invalid bucket name!");
    assert!(
        invalid_bucket.is_err(),
        "Should fail with invalid bucket name"
    );
    println!("✓ Invalid bucket name validation working");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Background heal status API requires distributed MinIO deployment (not available in single-node 'xl-single' mode)
async fn test_background_heal_status() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: BackgroundHealStatusResponse = madmin_client
        .background_heal_status()
        .build()
        .send()
        .await
        .expect("Failed to get background heal status");

    let status = resp.status().unwrap();
    println!("✓ Scanned items count: {}", status.scanned_items_count);

    if let Some(offline_endpoints) = &status.offline_endpoints {
        if !offline_endpoints.is_empty() {
            println!("⚠ Offline endpoints: {}", offline_endpoints.len());
            for endpoint in offline_endpoints {
                println!("  - {}", endpoint);
            }
        } else {
            println!("✓ No offline endpoints");
        }
    }

    if let Some(heal_disks) = &status.heal_disks
        && !heal_disks.is_empty()
    {
        println!("Healing disks: {}", heal_disks.len());
        for disk in heal_disks {
            println!("  - {}", disk);
        }
    }

    if let Some(sets) = &status.sets {
        println!("Healing sets: {}", sets.len());
        for set_status in sets {
            println!("  Pool {}, Set {}:", set_status.pool, set_status.set);
            println!("    Objects healed: {}", set_status.objects_healed);
            println!("    Objects failed: {}", set_status.objects_failed);
            println!("    Bytes healed: {}", set_status.bytes_healed);
            println!("    Bytes failed: {}", set_status.bytes_failed);
        }
    }

    if let Some(mrf) = &status.mrf {
        println!("MRF status: {} entries", mrf.len());
        for (endpoint, mrf_status) in mrf {
            println!("  {}:", endpoint);
            println!("    Items healed: {}", mrf_status.items_healed);
            println!("    Bytes healed: {}", mrf_status.bytes_healed);
            println!("    Total items: {}", mrf_status.total_items);
            println!("    Total bytes: {}", mrf_status.total_bytes);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Heal APIs require distributed MinIO deployment (not available in single-node 'xl-single' mode)
async fn test_heal_with_deep_scan() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider.clone()));

    let test_bucket_str = format!("test-heal-deep-{}", chrono::Utc::now().timestamp());
    let test_bucket = BucketName::try_from(test_bucket_str.as_str()).unwrap();

    let s3_provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let s3_client = MinioClient::new(ctx.base_url.clone(), Some(s3_provider), None, None).unwrap();

    s3_client
        .create_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create test bucket");

    // Test with Deep scan mode
    let opts = HealOpts {
        recursive: Some(true),
        dry_run: Some(true),
        scan_mode: Some(HealScanMode::Deep),
        ..Default::default()
    };

    let resp: HealResponse = madmin_client
        .heal()
        .bucket(Some(test_bucket.clone()))
        .opts(opts)
        .force_start(true)
        .build()
        .send()
        .await
        .expect("Failed to start heal operation with deep scan");

    let result = resp.result().unwrap();
    match result {
        HealResult::Start(start_info) => {
            println!("✓ Heal with deep scan started successfully");
            println!("  Client token: {}", start_info.client_token);
            assert!(!start_info.client_token.is_empty());
        }
        HealResult::Status(_) => {
            panic!("Expected HealResult::Start, got HealResult::Status");
        }
    }

    s3_client
        .delete_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove test bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Heal APIs require distributed MinIO deployment (not available in single-node 'xl-single' mode)
async fn test_heal_with_pool_and_set() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider.clone()));

    let test_bucket_str = format!("test-heal-pool-set-{}", chrono::Utc::now().timestamp());
    let test_bucket = BucketName::try_from(test_bucket_str.as_str()).unwrap();

    let s3_provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let s3_client = MinioClient::new(ctx.base_url.clone(), Some(s3_provider), None, None).unwrap();

    s3_client
        .create_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create test bucket");

    // Test with pool and set targeting
    let opts = HealOpts {
        recursive: Some(true),
        dry_run: Some(true),
        scan_mode: Some(HealScanMode::Normal),
        pool: Some(0),
        set: Some(0),
        ..Default::default()
    };

    let resp: HealResponse = madmin_client
        .heal()
        .bucket(Some(test_bucket.clone()))
        .opts(opts)
        .force_start(true)
        .build()
        .send()
        .await
        .expect("Failed to start heal operation with pool and set");

    let result = resp.result().unwrap();
    match result {
        HealResult::Start(start_info) => {
            println!("✓ Heal with pool and set started successfully");
            println!("  Client token: {}", start_info.client_token);
            assert!(!start_info.client_token.is_empty());
        }
        HealResult::Status(_) => {
            panic!("Expected HealResult::Start, got HealResult::Status");
        }
    }

    s3_client
        .delete_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove test bucket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Heal APIs require distributed MinIO deployment (not available in single-node 'xl-single' mode)
async fn test_heal_with_update_parity() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider.clone()));

    let test_bucket_str = format!("test-heal-parity-{}", chrono::Utc::now().timestamp());
    let test_bucket = BucketName::try_from(test_bucket_str.as_str()).unwrap();

    let s3_provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let s3_client = MinioClient::new(ctx.base_url.clone(), Some(s3_provider), None, None).unwrap();

    s3_client
        .create_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create test bucket");

    // Test with update_parity option
    let opts = HealOpts {
        recursive: Some(true),
        dry_run: Some(false), // Must be false for update_parity to take effect
        scan_mode: Some(HealScanMode::Normal),
        update_parity: Some(true),
        ..Default::default()
    };

    let resp: HealResponse = madmin_client
        .heal()
        .bucket(Some(test_bucket.clone()))
        .opts(opts)
        .force_start(true)
        .build()
        .send()
        .await
        .expect("Failed to start heal operation with update_parity");

    let result = resp.result().unwrap();
    match result {
        HealResult::Start(start_info) => {
            println!("✓ Heal with update_parity started successfully");
            println!("  Client token: {}", start_info.client_token);
            assert!(!start_info.client_token.is_empty());
        }
        HealResult::Status(_) => {
            panic!("Expected HealResult::Start, got HealResult::Status");
        }
    }

    s3_client
        .delete_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove test bucket");
}

#[test]
fn test_heal_result_item_helper_methods() {
    use minio::madmin::types::heal::{
        DriveState, HealDriveInfo, HealDriveState, HealItemType, HealResultItem,
    };

    let item = HealResultItem {
        result_index: 1,
        type_: HealItemType::Object,
        bucket: "test-bucket".to_string(),
        object: "test-object".to_string(),
        version_id: "".to_string(),
        detail: "".to_string(),
        parity_blocks: Some(4),
        data_blocks: Some(8),
        disk_count: 12,
        set_count: 1,
        before: HealDriveState {
            drives: vec![
                HealDriveInfo {
                    uuid: "drive1".to_string(),
                    endpoint: "http://localhost:9001".to_string(),
                    state: DriveState::Ok,
                },
                HealDriveInfo {
                    uuid: "drive2".to_string(),
                    endpoint: "http://localhost:9002".to_string(),
                    state: DriveState::Missing,
                },
                HealDriveInfo {
                    uuid: "drive3".to_string(),
                    endpoint: "http://localhost:9003".to_string(),
                    state: DriveState::Corrupted,
                },
                HealDriveInfo {
                    uuid: "drive4".to_string(),
                    endpoint: "http://localhost:9004".to_string(),
                    state: DriveState::Offline,
                },
            ],
        },
        after: HealDriveState {
            drives: vec![
                HealDriveInfo {
                    uuid: "drive1".to_string(),
                    endpoint: "http://localhost:9001".to_string(),
                    state: DriveState::Ok,
                },
                HealDriveInfo {
                    uuid: "drive2".to_string(),
                    endpoint: "http://localhost:9002".to_string(),
                    state: DriveState::Ok,
                },
                HealDriveInfo {
                    uuid: "drive3".to_string(),
                    endpoint: "http://localhost:9003".to_string(),
                    state: DriveState::Ok,
                },
                HealDriveInfo {
                    uuid: "drive4".to_string(),
                    endpoint: "http://localhost:9004".to_string(),
                    state: DriveState::Ok,
                },
            ],
        },
        object_size: 1024,
    };

    // Test missing counts
    let (before_missing, after_missing) = item.get_missing_counts();
    assert_eq!(
        before_missing, 1,
        "Should have 1 missing drive before healing"
    );
    assert_eq!(
        after_missing, 0,
        "Should have 0 missing drives after healing"
    );

    // Test corrupted counts
    let (before_corrupted, after_corrupted) = item.get_corrupted_counts();
    assert_eq!(
        before_corrupted, 1,
        "Should have 1 corrupted drive before healing"
    );
    assert_eq!(
        after_corrupted, 0,
        "Should have 0 corrupted drives after healing"
    );

    // Test offline counts
    let (before_offline, after_offline) = item.get_offline_counts();
    assert_eq!(
        before_offline, 1,
        "Should have 1 offline drive before healing"
    );
    assert_eq!(
        after_offline, 0,
        "Should have 0 offline drives after healing"
    );

    // Test online counts
    let (before_online, after_online) = item.get_online_counts();
    assert_eq!(
        before_online, 1,
        "Should have 1 online drive before healing"
    );
    assert_eq!(after_online, 4, "Should have 4 online drives after healing");

    println!("✓ All HealResultItem helper methods working correctly");
}
