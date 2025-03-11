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

use crate::common::{RandReader, RandSrc, TestContext, create_bucket_helper, rand_object_name};
use http::header;
use minio::s3::args::{PutObjectArgs, StatObjectArgs};
use minio::s3::builders::ObjectContent;
use minio::s3::error::Error;
use minio::s3::types::S3Api;
use tokio::sync::mpsc;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size = 16_usize;
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
    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.bucket_name, bucket_name);
    assert_eq!(resp.object_name, object_name);
    assert_eq!(resp.size, size);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    // Validate delete succeeded.
    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await;
    match resp.err().unwrap() {
        Error::S3Error(er) => {
            assert_eq!(er.code, "NoSuchKey")
        }
        e => panic!("Unexpected error {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object_multipart() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size: usize = 16 + 5 * 1024 * 1024;
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
    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.bucket_name, bucket_name);
    assert_eq!(resp.object_name, object_name);
    assert_eq!(resp.size, size);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object_content() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let sizes = [16_u64, 5 * 1024 * 1024, 16 + 5 * 1024 * 1024];

    for size in sizes.iter() {
        let data_src = RandSrc::new(*size);
        let rsp = ctx
            .client
            .put_object_content(
                &bucket_name,
                &object_name,
                ObjectContent::new_from_stream(data_src, Some(*size)),
            )
            .content_type(String::from("image/jpeg"))
            .send()
            .await
            .unwrap();
        assert_eq!(rsp.object_size, *size);
        let etag = rsp.etag;
        let resp = ctx
            .client
            .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.size, *size as usize);
        assert_eq!(resp.etag, etag);
        assert_eq!(
            resp.headers.get(header::CONTENT_TYPE).unwrap(),
            "image/jpeg"
        );
        ctx.client
            .remove_object(&bucket_name, object_name.as_str())
            .send()
            .await
            .unwrap();
    }

    // Repeat test with no size specified in ObjectContent
    for size in sizes.iter() {
        let data_src = RandSrc::new(*size);
        let rsp = ctx
            .client
            .put_object_content(
                &bucket_name,
                &object_name,
                ObjectContent::new_from_stream(data_src, None),
            )
            .part_size(Some(5 * 1024 * 1024)) // Set part size to 5MB
            .send()
            .await
            .unwrap();
        assert_eq!(rsp.object_size, *size);
        let etag = rsp.etag;
        let resp = ctx
            .client
            .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.size, *size as usize);
        assert_eq!(resp.etag, etag);
        ctx.client
            .remove_object(&bucket_name, object_name.as_str())
            .send()
            .await
            .unwrap();
    }
}

/// Test sending ObjectContent across async tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object_content_2() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();
    let sizes = vec![16_u64, 5 * 1024 * 1024, 16 + 5 * 1024 * 1024];

    let (sender, mut receiver): (mpsc::Sender<ObjectContent>, mpsc::Receiver<ObjectContent>) =
        mpsc::channel(2);

    let sender_handle = {
        let sizes = sizes.clone();
        tokio::spawn(async move {
            for size in sizes.iter() {
                let data_src = RandSrc::new(*size);
                sender
                    .send(ObjectContent::new_from_stream(data_src, Some(*size)))
                    .await
                    .unwrap();
            }
        })
    };

    let uploader_handler = {
        let sizes = sizes.clone();
        let object_name = object_name.clone();
        let client = ctx.client.clone();
        let test_bucket = bucket_name.clone();
        tokio::spawn(async move {
            let mut idx = 0;
            while let Some(item) = receiver.recv().await {
                let rsp = client
                    .put_object_content(&test_bucket, &object_name, item)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(rsp.object_size, sizes[idx]);
                let etag = rsp.etag;
                let resp = client
                    .stat_object(&StatObjectArgs::new(&test_bucket, &object_name).unwrap())
                    .await
                    .unwrap();
                assert_eq!(resp.size, sizes[idx] as usize);
                assert_eq!(resp.etag, etag);
                client
                    .remove_object(&test_bucket, object_name.as_str())
                    .send()
                    .await
                    .unwrap();

                idx += 1;
            }
        })
    };

    sender_handle.await.unwrap();
    uploader_handler.await.unwrap();
}
