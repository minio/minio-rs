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

use crate::common::{CleanupGuard, TestContext, rand_bucket_name, rand_object_name};
use bytes::Bytes;
use minio::s3::args::{
    DisableObjectLegalHoldArgs, EnableObjectLegalHoldArgs, IsObjectLegalHoldEnabledArgs,
};
use minio::s3::response::{
    DisableObjectLegalHoldResponse, EnableObjectLegalHoldResponse,
    IsObjectLegalHoldEnabledResponse, MakeBucketResponse, PutObjectContentResponse,
    SetObjectLockConfigResponse,
};
use minio::s3::types::S3Api;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_legal_hold() {
    let ctx = TestContext::new_from_env();
    let bucket_name: String = rand_bucket_name();
    let _resp: MakeBucketResponse = ctx
        .client
        .make_bucket(&bucket_name)
        .object_lock(true)
        .send()
        .await
        .unwrap();
    let _cleanup = CleanupGuard::new(&ctx, &bucket_name);
    let object_name = rand_object_name();

    let data = Bytes::from("hello, world".to_string().into_bytes());
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .send()
        .await
        .unwrap();
    //println!("response of put object content: resp={:?}", resp);

    let _resp: DisableObjectLegalHoldResponse = ctx
        .client
        .disable_object_legal_hold(
            &DisableObjectLegalHoldArgs::new(&bucket_name, &object_name).unwrap(),
        )
        .await
        .unwrap();
    //println!("response of setting object legal hold: resp={:?}", resp);

    let resp: IsObjectLegalHoldEnabledResponse = ctx
        .client
        .is_object_legal_hold_enabled(
            &IsObjectLegalHoldEnabledArgs::new(&bucket_name, &object_name).unwrap(),
        )
        .await
        .unwrap();
    //println!("response of getting object legal hold: resp={:?}", resp);
    assert!(!resp.enabled);

    let _resp: EnableObjectLegalHoldResponse = ctx
        .client
        .enable_object_legal_hold(
            &EnableObjectLegalHoldArgs::new(&bucket_name, &object_name).unwrap(),
        )
        .await
        .unwrap();
    //println!("response of setting object legal hold: resp={:?}", resp);

    let resp = ctx
        .client
        .is_object_legal_hold_enabled(
            &IsObjectLegalHoldEnabledArgs::new(&bucket_name, &object_name).unwrap(),
        )
        .await
        .unwrap();
    //println!("response of getting object legal hold: resp={:?}", resp);
    assert!(resp.enabled);
}
