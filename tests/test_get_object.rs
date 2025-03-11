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

use crate::common::{TestContext, create_bucket_helper, rand_object_name};
use bytes::Bytes;
use minio::s3::args::{GetObjectArgs, PutObjectArgs};
use minio::s3::types::S3Api;
use std::io::BufReader;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let data = Bytes::from("hello, world".to_string().into_bytes());
    ctx.client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .send()
        .await
        .unwrap();
    let resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    let got = resp.content.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(got, data);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_object_old() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let data = "hello, world";
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut BufReader::new(data.as_bytes()),
                Some(data.len()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let resp = ctx
        .client
        .get_object_old(&GetObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    let got = resp.text().await.unwrap();
    assert_eq!(got, data);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}
