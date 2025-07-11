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

use http::header;
use minio::s3::builders::Size::Known;
use minio::s3::builders::{MIN_PART_SIZE, ObjectContent};
use minio::s3::response::a_response_traits::{
    HasBucket, HasEtagFromHeaders, HasIsDeleteMarker, HasObject, HasS3Fields,
};
use minio::s3::response::{DeleteObjectResponse, PutObjectContentResponse, StatObjectResponse};
use minio::s3::types::S3Api;
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use tokio::sync::mpsc;

async fn test_put_object(ctx: &TestContext, bucket_name: &str, object_name: &str) {
    let size = 16_u64;
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            bucket_name,
            object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), size);

    let resp: StatObjectResponse = ctx
        .client
        .stat_object(bucket_name, object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.size().unwrap(), size);
}

/// Test putting an object into a bucket and verifying its existence.
#[minio_macros::test]
async fn put_object_1(ctx: TestContext, bucket_name: String) {
    test_put_object(&ctx, &bucket_name, &rand_object_name()).await;
}

/// Test putting an object with a name that contains special characters.
#[minio_macros::test]
async fn put_object_2(ctx: TestContext, bucket_name: String) {
    test_put_object(&ctx, &bucket_name, "name with+spaces").await;
    test_put_object(&ctx, &bucket_name, "name%20with%2Bspaces").await;
}

#[minio_macros::test]
async fn put_object_multipart(ctx: TestContext, bucket_name: String) {
    let object_name: String = rand_object_name();

    let size: u64 = 16 + MIN_PART_SIZE;

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), size);

    let resp: StatObjectResponse = ctx
        .client
        .stat_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.size().unwrap(), size);
}

#[minio_macros::test]
async fn put_object_content_1(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let sizes = [16_u64, MIN_PART_SIZE, 16 + MIN_PART_SIZE];

    for size in sizes.iter() {
        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(
                &bucket_name,
                &object_name,
                ObjectContent::new_from_stream(RandSrc::new(*size), Some(*size)),
            )
            .content_type(String::from("image/jpeg"))
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.object_size(), *size);

        let etag = resp.etag().unwrap();
        let resp: StatObjectResponse = ctx
            .client
            .stat_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.size().unwrap(), *size);
        assert_eq!(resp.etag().unwrap(), etag);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/jpeg"
        );

        let resp: DeleteObjectResponse = ctx
            .client
            .delete_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();

        assert!(!resp.is_delete_marker().unwrap());
    }
}

#[minio_macros::test]
async fn put_object_content_2(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let sizes = [16_u64, MIN_PART_SIZE, 16 + MIN_PART_SIZE];

    // Repeat test with no size specified in ObjectContent
    for size in sizes.iter() {
        let data_src = RandSrc::new(*size);
        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(
                &bucket_name,
                &object_name,
                ObjectContent::new_from_stream(data_src, None),
            )
            .part_size(Known(MIN_PART_SIZE))
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.object_size(), *size);
        let etag = resp.etag().unwrap();

        let resp: StatObjectResponse = ctx
            .client
            .stat_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.size().unwrap(), *size);
        assert_eq!(resp.etag().unwrap(), etag);
    }
}

/// Test sending PutObject across async tasks.
#[minio_macros::test]
async fn put_object_content_3(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let sizes = vec![16_u64, MIN_PART_SIZE, 16 + MIN_PART_SIZE];

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
                let resp: PutObjectContentResponse = client
                    .put_object_content(&test_bucket, &object_name, item)
                    .build()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.object_size(), sizes[idx]);
                let etag = resp.etag().unwrap();
                let resp: StatObjectResponse = client
                    .stat_object(&test_bucket, &object_name)
                    .build()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.size().unwrap(), sizes[idx]);
                assert_eq!(resp.etag().unwrap(), etag);

                idx += 1;
            }
        })
    };

    sender_handle.await.unwrap();
    uploader_handler.await.unwrap();
}
