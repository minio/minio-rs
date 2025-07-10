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
use minio::s3::response::a_response_traits::{HasBucket, HasObject};
use minio::s3::response::{
    DeleteObjectResponse, DeleteObjectsResponse, DeleteResult, PutObjectContentResponse,
};
use minio::s3::types::{S3Api, ToStream};
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name_utf8;

async fn create_object(
    ctx: &TestContext,
    bucket_name: &str,
    object_name: &str,
) -> PutObjectContentResponse {
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(bucket_name, object_name, "hello world")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    resp
}

#[minio_macros::test]
async fn delete_object(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name_utf8(20);
    let _resp = create_object(&ctx, &bucket_name, &object_name).await;

    let resp: DeleteObjectResponse = ctx
        .client
        .delete_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
}

#[minio_macros::test]
async fn delete_object_with_whitespace(ctx: TestContext, bucket_name: String) {
    let object_name = format!(" {}", rand_object_name_utf8(20));
    let _resp = create_object(&ctx, &bucket_name, &object_name).await;

    let resp: DeleteObjectResponse = ctx
        .client
        .delete_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
}

#[minio_macros::test]
async fn delete_objects(ctx: TestContext, bucket_name: String) {
    const OBJECT_COUNT: usize = 3;
    let mut names: Vec<String> = Vec::new();
    for _ in 1..=OBJECT_COUNT {
        let object_name = rand_object_name_utf8(20);
        let _resp = create_object(&ctx, &bucket_name, &object_name).await;
        names.push(object_name);
    }
    let del_items: Vec<ObjectToDelete> = names
        .iter()
        .map(|v| ObjectToDelete::from(v.as_str()))
        .collect();

    let resp: DeleteObjectsResponse = ctx
        .client
        .delete_objects::<&String, ObjectToDelete>(&bucket_name, del_items)
        .verbose_mode(true) // Enable verbose mode to get detailed response
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
async fn delete_objects_streaming(ctx: TestContext, bucket_name: String) {
    const OBJECT_COUNT: usize = 3;
    let mut names: Vec<String> = Vec::new();
    for _ in 1..=OBJECT_COUNT {
        let object_name = rand_object_name_utf8(20);
        let _resp = create_object(&ctx, &bucket_name, &object_name).await;
        names.push(object_name);
    }
    let del_items: Vec<ObjectToDelete> = names
        .iter()
        .map(|v| ObjectToDelete::from(v.as_str()))
        .collect();

    let mut resp = ctx
        .client
        .delete_objects_streaming(&bucket_name, del_items.into_iter())
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
    assert_eq!(del_count, 3);
}
