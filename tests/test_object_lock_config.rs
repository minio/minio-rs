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

mod common;

use crate::common::{CleanupGuard, TestContext, rand_bucket_name};
use minio::s3::args::{
    DeleteObjectLockConfigArgs, GetObjectLockConfigArgs, SetObjectLockConfigArgs,
};
use minio::s3::response::{
    DeleteObjectLockConfigResponse, GetObjectLockConfigResponse, SetObjectLockConfigResponse,
};
use minio::s3::types::{ObjectLockConfig, RetentionMode, S3Api};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_object_lock_config() {
    let ctx = TestContext::new_from_env();
    let bucket_name: String = rand_bucket_name();
    ctx.client
        .make_bucket(&bucket_name)
        .object_lock(true)
        .send()
        .await
        .unwrap();
    let _cleanup = CleanupGuard::new(&ctx, &bucket_name);

    let _resp: SetObjectLockConfigResponse = ctx
        .client
        .set_object_lock_config(
            &SetObjectLockConfigArgs::new(
                &bucket_name,
                &ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(7), None).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let resp: GetObjectLockConfigResponse = ctx
        .client
        .get_object_lock_config(&GetObjectLockConfigArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(match resp.config.retention_mode {
        Some(r) => matches!(r, RetentionMode::GOVERNANCE),
        _ => false,
    });

    assert_eq!(resp.config.retention_duration_days, Some(7));
    assert!(resp.config.retention_duration_years.is_none());

    let _resp: DeleteObjectLockConfigResponse = ctx
        .client
        .delete_object_lock_config(&DeleteObjectLockConfigArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let resp: GetObjectLockConfigResponse = ctx
        .client
        .get_object_lock_config(&GetObjectLockConfigArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.config.retention_mode.is_none());
}
