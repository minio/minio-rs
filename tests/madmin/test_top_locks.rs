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
use minio::madmin::response::TopLocksResponse;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // TopLocks API requires distributed MinIO deployment (not available in 'xl-single' mode)
async fn test_top_locks() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: TopLocksResponse = madmin_client
        .top_locks()
        .build()
        .send()
        .await
        .expect("Failed to get top locks");

    let locks = resp.locks().unwrap();
    for lock in &locks {
        assert!(
            !lock.resource.is_empty(),
            "Lock resource should not be empty"
        );
        assert!(!lock.lock_type.is_empty(), "Lock type should not be empty");
        assert!(!lock.source.is_empty(), "Lock source should not be empty");
    }

    println!("✓ Retrieved {} locks", locks.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // TopLocks API requires distributed MinIO deployment (not available in 'xl-single' mode)
async fn test_top_locks_with_count() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: TopLocksResponse = madmin_client
        .top_locks()
        .count(5)
        .build()
        .send()
        .await
        .expect("Failed to get top locks with count=5");

    println!("✓ Retrieved {} locks (max 5)", resp.locks().unwrap().len());

    // The response should have at most 5 locks
    assert!(resp.locks().unwrap().len() <= 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
#[ignore] // TopLocks API requires distributed MinIO deployment (not available in 'xl-single' mode)
async fn test_top_locks_with_stale() {
    let ctx = TestContext::new_from_env();

    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    let resp: TopLocksResponse = madmin_client
        .top_locks()
        .stale(true)
        .build()
        .send()
        .await
        .expect("Failed to get top locks including stale");

    println!(
        "✓ Retrieved {} locks (including stale)",
        resp.locks().unwrap().len()
    );
}
