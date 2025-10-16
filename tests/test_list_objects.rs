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
use minio::s3::response::a_response_traits::{HasBucket, HasObject};
use minio::s3::response::{ListObjectsResponse, PutObjectContentResponse};
use minio::s3::types::ToStream;
use minio_common::test_context::TestContext;
use minio_common::utils::{rand_object_name, rand_object_name_utf8};
use std::collections::HashSet;

async fn test_list_objects(
    use_api_v1: bool,
    include_versions: bool,
    express: bool,
    n_prefixes: usize,
    n_objects: usize,
    ctx: TestContext,
    bucket_name: String,
) {
    if express {
        if use_api_v1 {
            panic!("S3-Express does not support V1 API");
        }
        if include_versions {
            panic!("S3-Express does not support versioning");
        }
    }

    let is_express = ctx.client.is_minio_express().await;
    if is_express && !express {
        eprintln!("Skipping test because it is running in MinIO Express mode");
        return;
    } else if !is_express && express {
        eprintln!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let mut names_set_before: HashSet<String> = HashSet::new();
    let mut names_vec_after: Vec<String> = Vec::with_capacity(n_prefixes * n_objects);

    for _ in 0..n_prefixes {
        let prefix: String = rand_object_name();
        for _ in 0..=n_objects {
            let object_name: String = format!("{}/{}", prefix, rand_object_name());
            let resp: PutObjectContentResponse = ctx
                .client
                .put_object_content(&bucket_name, &object_name, "hello world")
                .build()
                .send()
                .await
                .unwrap();
            assert_eq!(resp.bucket(), bucket_name);
            assert_eq!(resp.object(), object_name);
            names_set_before.insert(object_name);
        }
    }
    let mut stream = ctx
        .client
        .list_objects(&bucket_name)
        .use_api_v1(use_api_v1)
        .include_versions(include_versions)
        .recursive(true)
        .build()
        .to_stream()
        .await;

    while let Some(items) = stream.next().await {
        let items = items.unwrap().contents;
        for item in items.iter() {
            names_vec_after.push(item.name.clone());
        }
    }
    assert_eq!(names_vec_after.len(), names_set_before.len());
    let is_sorted: bool = names_vec_after.iter().is_sorted();

    if express {
        // we do not expect the results to be sorted, but it still might be
    } else {
        // we expect the results to be sorted
        assert!(
            is_sorted,
            "With regular (non S3-Express) we expected the results to be sorted, yet the list of objects is unsorted"
        );
    }
    let names_set_after: HashSet<String> = names_vec_after.into_iter().collect();
    assert_eq!(names_set_after, names_set_before);
}

#[minio_macros::test(skip_if_express)]
async fn list_objects_v1_no_versions(ctx: TestContext, bucket_name: String) {
    test_list_objects(true, false, false, 5, 5, ctx, bucket_name).await;
}

#[minio_macros::test(skip_if_express)]
async fn list_objects_v1_with_versions(ctx: TestContext, bucket_name: String) {
    test_list_objects(true, true, false, 5, 5, ctx, bucket_name).await;
}

#[minio_macros::test(skip_if_express)]
async fn list_objects_v2_no_versions(ctx: TestContext, bucket_name: String) {
    test_list_objects(false, false, false, 5, 5, ctx, bucket_name).await;
}

#[minio_macros::test(skip_if_express)]
async fn list_objects_v2_with_versions(ctx: TestContext, bucket_name: String) {
    test_list_objects(false, true, false, 5, 5, ctx, bucket_name).await;
}

/// Test for S3-Express: List objects with S3-Express are only supported with V2 API, without
/// versions, and yield results that need not be sorted.
#[minio_macros::test(skip_if_not_express)]
async fn list_objects_express(ctx: TestContext, bucket_name: String) {
    test_list_objects(false, false, true, 5, 5, ctx, bucket_name).await;
}

async fn test_list_one_object(ctx: &TestContext, bucket_name: &str, object_name: &str) {
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(bucket_name, object_name, "Hello, World!")
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);

    let mut stream = ctx
        .client
        .list_objects(bucket_name)
        .use_api_v1(false) // S3-Express does not support V1 API
        .include_versions(false) // S3-Express does not support versions
        .build()
        .to_stream()
        .await;

    let mut result: Vec<ListObjectsResponse> = Vec::new();
    while let Some(items) = stream.next().await {
        result.push(items.unwrap());
    }

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].contents[0].name, object_name);
}

/// Test listing an object with a name that contains utf-8 characters.
#[minio_macros::test]
async fn list_object_1(ctx: TestContext, bucket_name: String) {
    test_list_one_object(&ctx, &bucket_name, &rand_object_name_utf8(20)).await;
}

/// Test getting an object with a name that contains white space characters.
///
/// In percent-encoding, "a b+c" becomes "a%20b%2Bc", but some S3 implementations may do
/// form-encoding, yielding "a+b2Bc", which will result in "a+b+c" is percent-decoding is
/// used. This test checks that form-decoding is used to retrieve "a b+c".
#[minio_macros::test]
async fn list_object_2(ctx: TestContext, bucket_name: String) {
    test_list_one_object(&ctx, &bucket_name, "a b+c").await;
}
