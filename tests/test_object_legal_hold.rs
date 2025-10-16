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
use minio::s3::response::a_response_traits::{HasBucket, HasObject, HasRegion, HasVersion};
use minio::s3::response::{
    GetObjectLegalHoldResponse, PutObjectContentResponse, PutObjectLegalHoldResponse,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

#[minio_macros::test(skip_if_express, object_lock)]
async fn object_legal_hold_s3(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();

    let data = Bytes::from("hello, world".to_string().into_bytes());
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), data.len() as u64);

    let resp: PutObjectLegalHoldResponse = ctx
        .client
        .put_object_legal_hold(&bucket_name, &object_name, true)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert_eq!(resp.version_id(), None);

    let resp: GetObjectLegalHoldResponse = ctx
        .client
        .get_object_legal_hold(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.enabled().unwrap());
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert_eq!(resp.version_id(), None);

    let resp: PutObjectLegalHoldResponse = ctx
        .client
        .put_object_legal_hold(&bucket_name, &object_name, true)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert_eq!(resp.version_id(), None);

    let resp: GetObjectLegalHoldResponse = ctx
        .client
        .get_object_legal_hold(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.enabled().unwrap());
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert_eq!(resp.version_id(), None);
}
