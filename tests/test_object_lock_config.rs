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

use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{
    DeleteObjectLockConfigResponse, GetObjectLockConfigResponse, PutObjectLockConfigResponse,
};
use minio::s3::types::{ObjectLockConfig, RetentionMode, S3Api};
use minio_common::cleanup_guard::CleanupGuard;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_bucket_name;

#[tokio::test(flavor = "multi_thread")]
async fn object_lock_config() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express().await {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }

    let bucket_name: String = rand_bucket_name();
    ctx.client
        .create_bucket(&bucket_name)
        .object_lock(true)
        .send()
        .await
        .unwrap();
    let _cleanup = CleanupGuard::new(ctx.client.clone(), &bucket_name);

    const DURATION_DAYS: i32 = 7;
    let config =
        ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(DURATION_DAYS), None).unwrap();

    let resp: PutObjectLockConfigResponse = ctx
        .client
        .put_object_lock_config(&bucket_name)
        .config(config)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectLockConfigResponse = ctx
        .client
        .get_object_lock_config(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.config.retention_mode.unwrap(),
        RetentionMode::GOVERNANCE
    );
    assert_eq!(resp.config.retention_duration_days, Some(DURATION_DAYS));
    assert!(resp.config.retention_duration_years.is_none());
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: DeleteObjectLockConfigResponse = ctx
        .client
        .delete_object_lock_config(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectLockConfigResponse = ctx
        .client
        .get_object_lock_config(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.config.retention_mode.is_none());
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
}
