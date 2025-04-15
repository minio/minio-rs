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

use bytes::Bytes;

use minio::s3::client::DEFAULT_REGION;
use minio::s3::error::{Error, ErrorCode};
use minio::s3::response::{
    DisableObjectLegalHoldResponse, EnableObjectLegalHoldResponse,
    IsObjectLegalHoldEnabledResponse, MakeBucketResponse, PutObjectContentResponse,
};
use minio::s3::types::S3Api;
use minio_common::cleanup_guard::CleanupGuard;
use minio_common::test_context::TestContext;
use minio_common::utils::{rand_bucket_name, rand_object_name};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_legal_hold_s3() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express() {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }
    let bucket_name: String = rand_bucket_name();
    let _resp: MakeBucketResponse = ctx
        .client
        .make_bucket(&bucket_name)
        .object_lock(true)
        .send()
        .await
        .unwrap();
    let _cleanup = CleanupGuard::new(&ctx.client, &bucket_name);
    let object_name = rand_object_name();

    let data = Bytes::from("hello, world".to_string().into_bytes());
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, data.len() as u64);
    //println!("response of put object content: resp={:?}", resp);

    let resp: DisableObjectLegalHoldResponse = ctx
        .client
        .disable_object_legal_hold(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    //println!("response of setting object legal hold: resp={:?}", resp);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    assert_eq!(resp.version_id, None);

    let resp: IsObjectLegalHoldEnabledResponse = ctx
        .client
        .is_object_legal_hold_enabled(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    //println!("response of getting object legal hold: resp={:?}", resp);
    assert!(!resp.enabled);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    assert_eq!(resp.version_id, None);

    let resp: EnableObjectLegalHoldResponse = ctx
        .client
        .enable_object_legal_hold(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    //println!("response of setting object legal hold: resp={:?}", resp);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    assert_eq!(resp.version_id, None);

    let resp: IsObjectLegalHoldEnabledResponse = ctx
        .client
        .is_object_legal_hold_enabled(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    //println!("response of getting object legal hold: resp={:?}", resp);
    assert!(resp.enabled);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    assert_eq!(resp.version_id, None);
}
