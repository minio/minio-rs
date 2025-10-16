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
use minio::s3::response::a_response_traits::{HasBucket, HasRegion};
use minio::s3::response::{
    DeleteObjectLockConfigResponse, GetObjectLockConfigResponse, PutObjectLockConfigResponse,
};
use minio::s3::types::{ObjectLockConfig, RetentionMode, S3Api};
use minio_common::test_context::TestContext;

#[minio_macros::test(skip_if_express, object_lock)]
async fn object_lock_config(ctx: TestContext, bucket_name: String) {
    const DURATION_DAYS: i32 = 7;
    let config =
        ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(DURATION_DAYS), None).unwrap();

    let resp: PutObjectLockConfigResponse = ctx
        .client
        .put_object_lock_config(&bucket_name)
        .config(config)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetObjectLockConfigResponse = ctx
        .client
        .get_object_lock_config(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();

    let config = resp.config().unwrap();
    assert_eq!(config.retention_mode.unwrap(), RetentionMode::GOVERNANCE);
    assert_eq!(config.retention_duration_days, Some(DURATION_DAYS));
    assert!(config.retention_duration_years.is_none());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: DeleteObjectLockConfigResponse = ctx
        .client
        .delete_object_lock_config(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetObjectLockConfigResponse = ctx
        .client
        .get_object_lock_config(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    let config = resp.config().unwrap();
    assert!(config.retention_mode.is_none());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
}
