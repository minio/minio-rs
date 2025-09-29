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
use minio::s3::response::a_response_traits::{HasBucket, HasObject};
use minio::s3::response::{GetObjectResponse, PutObjectContentResponse};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name_utf8;

async fn test_get_object(ctx: &TestContext, bucket_name: &str, object_name: &str) {
    let data: Bytes = Bytes::from("hello, world".to_string().into_bytes());
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(bucket_name, object_name, data.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), data.len() as u64);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(bucket_name, object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size().unwrap(), data.len() as u64);

    let got = resp
        .content()
        .unwrap()
        .to_segmented_bytes()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(got, data);
}

/// Test getting an object with a name that contains utf-8 characters.
#[minio_macros::test]
async fn get_object_1(ctx: TestContext, bucket_name: String) {
    test_get_object(&ctx, &bucket_name, &rand_object_name_utf8(20)).await;
}

/// Test getting an object with a name that contains white space characters.
#[minio_macros::test]
async fn get_object_2(ctx: TestContext, bucket_name: String) {
    test_get_object(&ctx, &bucket_name, "a b+c").await;
}
