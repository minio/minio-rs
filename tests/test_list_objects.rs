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
use tokio_stream::StreamExt;

async fn list_objects(use_api_v1: bool, include_versions: bool) {
    const N_OBJECTS: usize = 3;
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let mut names: Vec<String> = Vec::new();
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
        names.push(object_name);
    }

    let mut stream = ctx
        .client
        .list_objects(&bucket_name)
        .use_api_v1(use_api_v1)
        .include_versions(include_versions)
        .to_stream()
        .await;

    let mut count = 0;
    while let Some(items) = stream.next().await {
        let items = items.unwrap().contents;
        for item in items.iter() {
            assert!(names.contains(&item.name));
            count += 1;
        }
    }
    assert_eq!(count, N_OBJECTS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v1_no_versions() {
    list_objects(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v1_with_versions() {
    list_objects(true, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v2_no_versions() {
    list_objects(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects_v2_with_versions() {
    list_objects(false, true).await;
}
