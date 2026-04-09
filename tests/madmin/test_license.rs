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
use minio::madmin::response::GetLicenseInfoResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_get_license_info() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Get license information
    let resp: GetLicenseInfoResponse = madmin_client
        .get_license_info()
        .build()
        .send()
        .await
        .expect("Failed to get license info");

    if !resp.license_key.is_empty() {
        assert!(resp.license_key.len() > 0, "License key length should be positive");
        println!("✓ License key present: {} chars", resp.license_key.len());
    } else {
        println!("✓ No license key (community edition or unlicensed)");
    }

    if let Some(expires_at) = &resp.expires_at {
        assert!(!expires_at.is_empty(), "Expires at should not be empty string");
        println!("✓ License expires at: {}", expires_at);
    }

    if let Some(organization) = &resp.organization {
        assert!(!organization.is_empty(), "Organization should not be empty string");
        println!("✓ Organization: {}", organization);
    }

    if let Some(email) = &resp.email {
        assert!(!email.is_empty(), "Email should not be empty string");
        println!("✓ Email: {}", email);
    }

    if let Some(plan) = &resp.plan {
        assert!(!plan.is_empty(), "Plan should not be empty string");
        println!("✓ Plan: {}", plan);
    }

    // Response should always be valid, even if empty for community edition
    println!("✓ GetLicenseInfo API call successful");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_get_license_info_community_edition() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Get license information
    let resp: GetLicenseInfoResponse = madmin_client
        .get_license_info()
        .build()
        .send()
        .await
        .expect("Failed to get license info");

    // For community edition servers, most fields will be empty
    // But the API should still succeed
    println!("License info response received");

    if resp.license_key.is_empty() && resp.organization.is_none() && resp.plan.is_none() {
        println!("✓ Community edition detected (no license info)");
    } else {
        println!("✓ Licensed edition detected");
        if let Some(plan) = &resp.plan {
            assert!(!plan.is_empty(), "Plan should not be empty string");
        }
    }
}
