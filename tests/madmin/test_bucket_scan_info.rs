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
use minio::madmin::response::BucketScanInfoResponse;
use minio::madmin::types::MadminApi;
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Newly created buckets don't have scan status yet (404 XMinioScanStatNotFound)
async fn test_bucket_scan_info() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider.clone()));

    let test_bucket = format!("test-scan-{}", chrono::Utc::now().timestamp());

    let s3_provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let s3_client = MinioClient::new(ctx.base_url.clone(), Some(s3_provider), None, None).unwrap();

    s3_client
        .create_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to create test bucket");

    let resp: BucketScanInfoResponse = madmin_client
        .bucket_scan_info(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to get bucket scan info");

    let scans = resp.scans().expect("Failed to parse scans");
    for (i, scan) in scans.iter().enumerate() {
        println!("Scan {}:", i + 1);
        println!("  Pool: {}", scan.pool);
        println!("  Set: {}", scan.set);
        println!("  Cycle: {}", scan.cycle);
        println!("  Ongoing: {}", scan.ongoing);
        println!("  Last Update: {}", scan.last_update);
        println!("  Last Started: {}", scan.last_started);

        if let Some(error) = &scan.last_error {
            assert!(!error.is_empty(), "Last error should not be empty string");
            println!("  Last Error: {}", error);
        }

        if let Some(completed) = &scan.completed {
            println!("  Completed scans: {}", completed.len());
        }
    }

    s3_client
        .delete_bucket(&test_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .expect("Failed to remove test bucket");

    println!("✓ Bucket scan info test completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_bucket_scan_info_invalid_bucket() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let result = madmin_client.bucket_scan_info("invalid bucket name!");

    // Validation should fail for invalid bucket name
    assert!(result.is_err(), "Should fail with invalid bucket name");
    println!("✓ Invalid bucket name validation working");
}
