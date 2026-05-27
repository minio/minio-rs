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
use minio::madmin::response::KmsStatusResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore = "KMS status API is not supported in MinIO mode-server-xl (requires KMS/KES configuration)"]
async fn test_kms_status() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: KmsStatusResponse = madmin_client
        .kms_status()
        .build()
        .send()
        .await
        .expect("Failed to get KMS status");

    // Verify KMS name is present
    assert!(
        !resp.status().unwrap().name.is_empty(),
        "KMS name should not be empty"
    );
    println!("KMS Name: {}", resp.status().unwrap().name);

    // Verify default key is configured
    assert!(
        !resp.status().unwrap().default_key.is_empty(),
        "Default KMS key should be configured"
    );
    println!("Default Key: {}", resp.status().unwrap().default_key);

    // Verify at least one endpoint is configured
    assert!(
        !resp.status().unwrap().endpoints.is_empty(),
        "At least one KMS endpoint should be configured"
    );
    println!("Endpoints: {:?}", resp.status().unwrap().endpoints);

    // Log additional details if available
    if !resp.status().unwrap().endpoints.is_empty() {
        println!(
            "  Number of endpoints: {}",
            resp.status().unwrap().endpoints.len()
        );
        for (idx, endpoint) in resp.status().unwrap().endpoints.iter().enumerate() {
            println!("  Endpoint {}: {}", idx + 1, endpoint);
        }
    }

    println!("✓ KMS is properly configured and accessible");
}

// Note: Additional comprehensive KMS tests (18 APIs) are in test_kms_extended.rs
// See: tests/madmin/test_kms_extended.rs for:
// - KMSMetrics, KMSAPIs, KMSVersion
// - CreateKey, DeleteKey, ImportKey, ListKeys, GetKeyStatus
// - SetKMSPolicy, AssignPolicy, DescribePolicy, GetPolicy, ListPolicies, DeletePolicy
// - DescribeIdentity, DescribeSelfIdentity, ListIdentities, DeleteIdentity
