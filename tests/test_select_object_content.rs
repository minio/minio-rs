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

use minio::s3::error::{Error, ErrorCode};
use minio::s3::response::{PutObjectContentResponse, SelectObjectContentResponse};
use minio::s3::types::{S3Api, SelectRequest};
use minio_common::example::{create_select_content_data, create_select_content_request};
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn select_object_content_s3() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express().await {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name: String = rand_object_name();
    let (select_body, select_data) = create_select_content_data();

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, select_body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let select_request: SelectRequest = create_select_content_request();

    let mut resp: SelectObjectContentResponse = ctx
        .client
        .select_object_content(&bucket_name, &object_name, select_request)
        .send()
        .await
        .unwrap();
    let mut got = String::new();
    let mut buf = [0_u8; 512];
    loop {
        let size = resp.read(&mut buf).await.unwrap();
        if size == 0 {
            break;
        }
        got += core::str::from_utf8(&buf[..size]).unwrap();
    }
    assert_eq!(got, select_data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn select_object_content_express() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express().await {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();
    let (select_body, _) = create_select_content_data();

    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, select_body)
        .send()
        .await
        .unwrap();

    let select_request: SelectRequest = create_select_content_request();

    let resp: Result<SelectObjectContentResponse, Error> = ctx
        .client
        .select_object_content(&bucket_name, &object_name, select_request)
        .send()
        .await;
    match resp {
        Err(Error::S3Error(e)) => assert_eq!(e.code, ErrorCode::NotSupported),
        v => panic!("Expected error S3Error(NotSupported): but got {:?}", v),
    }
}
