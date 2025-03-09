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

use crate::common::{create_bucket_helper, rand_object_name, RandReader, TestContext};
use minio::s3::args::PutObjectArgs;
use minio::s3::builders::ObjectToDelete;
use minio::s3::types::ToStream;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects() {
    const N_OBJECTS: usize = 3;
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let mut names: Vec<String> = Vec::new();
    for _ in 1..=N_OBJECTS {
        let object_name = rand_object_name();
        let size = 0_usize;
        ctx.client
            .put_object_old(
                &mut PutObjectArgs::new(
                    &bucket_name,
                    &object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        names.push(object_name);
    }

    let mut stream = ctx.client.list_objects(&bucket_name).to_stream().await;

    let mut count = 0;
    while let Some(items) = stream.next().await {
        let items = items.unwrap().contents;
        for item in items.iter() {
            assert!(names.contains(&item.name));
            count += 1;
        }
    }
    assert_eq!(count, N_OBJECTS);

    let del_items: Vec<ObjectToDelete> = names
        .iter()
        .map(|v| ObjectToDelete::from(v.as_str()))
        .collect();
    let mut resp = ctx
        .client
        .remove_objects(&bucket_name, del_items.into_iter())
        .verbose_mode(true)
        .to_stream()
        .await;
    while let Some(item) = resp.next().await {
        let res = item.unwrap();
        for obj in res.result.iter() {
            assert!(obj.is_deleted());
        }
    }
}
