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

use async_std::stream::StreamExt;
use minio::s3::builders::ObjectToDelete;
use minio::s3::response::{
    DeleteObjectResponse, DeleteObjectsResponse, DeleteResult, PutObjectContentResponse,
};
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{BucketName, ObjectKey, S3Api, ToStream};
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name_utf8;

async fn create_object_helper(
    ctx: &TestContext,
    bucket: &BucketName,
    object: &ObjectKey,
) -> PutObjectContentResponse {
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(bucket, object, "hello world")
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(bucket));
    assert_eq!(resp.object(), Some(object));
    resp
}

async fn test_delete_object(ctx: &TestContext, bucket: &BucketName, object: &ObjectKey) {
    let _resp = create_object_helper(ctx, bucket, object).await;

    let resp: DeleteObjectResponse = ctx
        .client
        .delete_object(bucket, object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), Some(bucket));
}

/// Test deleting an object with a name that contains utf-8 characters.
#[minio_macros::test]
async fn delete_object_1(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name_utf8(20);
    test_delete_object(&ctx, &bucket, &object).await;
}

/// Test deleting an object with a name that contains white space characters.
#[minio_macros::test]
async fn delete_object_2(ctx: TestContext, bucket: BucketName) {
    let object = ObjectKey::try_from("a b+c").unwrap();
    test_delete_object(&ctx, &bucket, &object).await;
}

#[minio_macros::test]
async fn delete_objects(ctx: TestContext, bucket: BucketName) {
    const OBJECT_COUNT: usize = 3;
    let mut names: Vec<ObjectKey> = Vec::new();
    for _ in 1..=OBJECT_COUNT {
        let object = rand_object_name_utf8(20);
        let _resp = create_object_helper(&ctx, &bucket, &object).await;
        names.push(object);
    }
    let del_items: Vec<ObjectToDelete> = names.iter().map(ObjectToDelete::from).collect();

    let resp: DeleteObjectsResponse = ctx
        .client
        .delete_objects(&bucket, del_items)
        .unwrap()
        .verbose_mode(true) // Enable verbose mode to get detailed response
        .build()
        .send()
        .await
        .unwrap();

    let deleted_names: Vec<DeleteResult> = resp.result().unwrap();
    assert_eq!(deleted_names.len(), OBJECT_COUNT);
    for obj in deleted_names.iter() {
        assert!(obj.is_deleted());
    }
}

#[minio_macros::test]
async fn delete_objects_streaming(ctx: TestContext, bucket: BucketName) {
    const OBJECT_COUNT: usize = 3;
    let mut names: Vec<ObjectKey> = Vec::new();
    for _ in 1..=OBJECT_COUNT {
        let object = rand_object_name_utf8(20);
        let _resp = create_object_helper(&ctx, &bucket, &object).await;
        names.push(object);
    }
    let del_items: Vec<ObjectToDelete> = names.iter().map(ObjectToDelete::from).collect();

    let mut resp = ctx
        .client
        .delete_objects_streaming(&bucket, del_items.into_iter())
        .unwrap()
        .verbose_mode(true)
        .to_stream()
        .await;

    let mut del_count = 0;
    while let Some(item) = resp.next().await {
        let res = item.unwrap();
        let del_result = res.result().unwrap();
        del_count += del_result.len();

        for obj in del_result.into_iter() {
            assert!(obj.is_deleted());
        }
    }
    assert_eq!(del_count, OBJECT_COUNT);
}
