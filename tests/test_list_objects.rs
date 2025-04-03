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

use minio::s3::response::PutObjectContentResponse;
use minio::s3::types::ToStream;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use std::collections::HashSet;
use test_tag::tag;
use tokio_stream::StreamExt;

async fn list_objects(use_api_v1: bool, include_versions: bool, unsorted: bool) {
    const N_OBJECTS: usize = 100;
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let mut names_set_before: HashSet<String> = HashSet::new();
    let mut names_vec_after: Vec<String> = Vec::with_capacity(N_OBJECTS);

    for _ in 1..=N_OBJECTS {
        let object_name: String = rand_object_name();
        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &object_name, "hello world")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.object, object_name);
        names_set_before.insert(object_name);
    }

    let mut stream = ctx
        .client
        .list_objects(&bucket_name)
        .use_api_v1(use_api_v1)
        .include_versions(include_versions)
        .unsorted(unsorted)
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

    if unsorted {
        // we expect the results to be unsorted
        assert!(
            !is_sorted,
            "expected the results to be unsorted, yet the list of objects is sorted"
        );
    } else {
        // we expect the results to be sorted
        assert!(
            is_sorted,
            "expected the results to be sorted, yet the list of objects is unsorted"
        );
    }
    let names_set_after: HashSet<String> = names_vec_after.into_iter().collect();
    assert_eq!(names_set_after, names_set_before);
}

#[tag(s3)]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v1_no_versions_sorted() {
    list_objects(true, false, false).await;
}

#[tag(s3)]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v1_with_versions_sorted() {
    list_objects(true, true, false).await;
}

#[tag(s3)]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v2_no_versions_sorted() {
    list_objects(false, false, false).await;
}

#[tag(s3)]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v2_with_versions_sorted() {
    list_objects(false, true, false).await;
}

#[tag(s3express)]
//#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v2_no_versions_unsorted() {
    list_objects(false, false, true).await;
}
