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
use minio::s3::response::{GetObjectResponse, PutObjectContentResponse};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let data: Bytes = Bytes::from("hello, world".to_string().into_bytes());
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, data.len() as u64);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, data.len() as u64);

    let got = resp.content.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(got, data);
}
