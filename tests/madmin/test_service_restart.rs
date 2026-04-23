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
use minio::madmin::response::ServiceRestartResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // Ignored by default as it restarts the MinIO server
async fn test_service_restart() {
    let ctx = TestContext::new_from_env();

    // Create MadminClient from the same base URL with credentials
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Restart service
    let _resp: ServiceRestartResponse = madmin_client
        .service_restart()
        .build()
        .send()
        .await
        .unwrap();

    // Success is indicated by no error
    println!("Service restart initiated successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_service_restart_unauthorized() {
    let ctx = TestContext::new_from_env();

    // Create a user without admin privileges
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // First create a non-admin user
    let username = format!(
        "readonly-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let password = "TestPassword123!";

    let _add_resp = madmin_client
        .add_user()
        .access_key(&username)
        .secret_key(password)
        .build()
        .send()
        .await
        .unwrap();

    // Try to restart service with the non-admin user credentials
    let provider2 = StaticProvider::new(&username, password, None);
    let madmin_client2 = MadminClient::new(ctx.base_url.clone(), Some(provider2));

    let result: Result<ServiceRestartResponse, _> =
        madmin_client2.service_restart().build().send().await;

    // Should fail with unauthorized error
    assert!(result.is_err());
    println!("Service restart correctly denied for non-admin user");

    // Clean up
    let _remove_resp = madmin_client
        .remove_user()
        .access_key(&username)
        .build()
        .send()
        .await
        .unwrap();

    println!("Cleaned up test user: {}", username);
}
