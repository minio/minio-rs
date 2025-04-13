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

use minio::s3::builders::ObjectContent;
use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{
    GetObjectRetentionResponse, MakeBucketResponse, PutObjectContentResponse,
    SetObjectRetentionResponse,
};
use minio::s3::types::{RetentionMode, S3Api};
use minio::s3::utils::{to_iso8601utc, utc_now};
use minio_common::cleanup_guard::CleanupGuard;
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::{rand_bucket_name, rand_object_name};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_retention() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express() {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }

    let bucket_name: String = rand_bucket_name();
    let resp: MakeBucketResponse = ctx
        .client
        .make_bucket(&bucket_name)
        .object_lock(true)
        .send()
        .await
        .unwrap();
    let _cleanup = CleanupGuard::new(&ctx.client, &bucket_name);
    assert_eq!(resp.bucket, bucket_name);
    let object_name = rand_object_name();

    let size = 16_u64;

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);
    assert_ne!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);
    //assert_eq!(resp.etag, "");

    let retain_until_date = utc_now() + chrono::Duration::days(1);
    let obj_resp: SetObjectRetentionResponse = ctx
        .client
        .set_object_retention(&bucket_name, &object_name)
        .retention_mode(Some(RetentionMode::GOVERNANCE))
        .retain_until_date(Some(retain_until_date))
        .send()
        .await
        .unwrap();
    assert_eq!(obj_resp.bucket, bucket_name);
    assert_eq!(obj_resp.object, object_name);
    assert_eq!(obj_resp.version_id, None);
    assert_eq!(obj_resp.region, DEFAULT_REGION);

    let resp: GetObjectRetentionResponse = ctx
        .client
        .get_object_retention(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.retention_mode.unwrap(), RetentionMode::GOVERNANCE);
    assert_eq!(
        to_iso8601utc(resp.retain_until_date.unwrap()),
        to_iso8601utc(retain_until_date)
    );

    let resp: SetObjectRetentionResponse = ctx
        .client
        .set_object_retention(&bucket_name, &object_name)
        .bypass_governance_mode(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectRetentionResponse = ctx
        .client
        .get_object_retention(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert!(resp.retention_mode.is_none());
    assert!(resp.retain_until_date.is_none());
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);
}
