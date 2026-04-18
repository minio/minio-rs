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

use minio::s3::builders::{MIN_PART_SIZE, ObjectContent};
use minio::s3::error::{Error, S3ServerError};
use minio::s3::minio_error_response::MinioErrorCode;
use minio::s3::response::{
    AppendObjectResponse, GetObjectResponse, PutObjectContentResponse, PutObjectResponse,
    StatObjectResponse,
};
use minio::s3::response_traits::{HasBucket, HasEtagFromHeaders, HasObject, HasObjectSize};
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::{BucketName, ETag, ObjectKey, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use tokio::sync::mpsc;

/// create an object with the given content and check that it is created correctly
async fn create_object_helper(
    content: &str,
    bucket: &BucketName,
    object: &ObjectKey,
    ctx: &TestContext,
) {
    let data: SegmentedBytes = SegmentedBytes::from(content.to_string());
    let size = content.len() as u64;
    // create an object (with put) that contains "aaaa"
    let resp: PutObjectResponse = ctx
        .client
        .put_object(bucket, object, data)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(bucket));
    assert_eq!(resp.object(), Some(object));

    let resp: GetObjectResponse = ctx
        .client
        .get_object(bucket, object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(bucket));
    assert_eq!(resp.object(), Some(object));
    assert_eq!(resp.object_size().unwrap(), size);

    // double-check that the content we just have put is "aaaa"
    let content1: String = String::from_utf8(
        resp.content()
            .unwrap()
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);
}

/// Append to the end of an existing object (happy flow)
#[minio_macros::test(skip_if_not_express)]
async fn append_object_0(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";
    let size = content1.len() as u64;

    create_object_helper(content1, &bucket, &object, &ctx).await;

    // now append "bbbb" to the end of the object
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());
    let offset_bytes = size;
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket, &object, data2, offset_bytes)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), size * 2);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket, &object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size().unwrap(), size * 2);

    // retrieve the content of the object and check that it is "aaaabbbb"
    let content: String = String::from_utf8(
        resp.content()
            .unwrap()
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, format!("{content1}{content2}"));
}

/// Append to the beginning of an existing object (happy flow)
#[minio_macros::test(skip_if_not_express)]
async fn append_object_1(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";
    let size = content1.len() as u64;

    create_object_helper(content1, &bucket, &object, &ctx).await;

    // now append "bbbb" to the beginning of the object
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());
    let offset_bytes = 0; // byte 0, thus the beginning of the file
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket, &object, data2, offset_bytes)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), size);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket, &object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size().unwrap(), size);

    // retrieve the content of the object and check that it is "bbbb"
    let content: String = String::from_utf8(
        resp.content()
            .unwrap()
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content2);
}

/// Append to the middle of an existing object (error InvalidWriteOffset)
#[minio_macros::test(skip_if_not_express)]
async fn append_object_2(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";
    let size = content1.len() as u64;

    create_object_helper(content1, &bucket, &object, &ctx).await;

    // now try to append "bbbb" to the object at an invalid offset
    let offset_bytes = size - 1;
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());
    let resp: Result<AppendObjectResponse, Error> = ctx
        .client
        .append_object(&bucket, &object, data2, offset_bytes)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(v) => panic!("append object should have failed; got value: {v:?}"),
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::InvalidWriteOffset);
        }
        Err(e) => panic!("append object should have failed; got error: {e:?}"),
    }
}

/// Append beyond the size of an existing object (error InvalidWriteOffset)
#[minio_macros::test(skip_if_not_express)]
async fn append_object_3(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";
    let size = content1.len() as u64;

    create_object_helper(content1, &bucket, &object, &ctx).await;

    // now try to append "bbbb" to the object at an invalid offset
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());
    let offset_bytes = size + 1;
    let resp: Result<AppendObjectResponse, Error> = ctx
        .client
        .append_object(&bucket, &object, data2, offset_bytes)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(v) => panic!("append object should have failed; got value: {v:?}"),
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::InvalidWriteOffset);
        }
        Err(e) => panic!("append object should have failed; got error: {e:?}"),
    }
}

/// Append to the beginning/end of a non-existing object (happy flow)
#[minio_macros::test(skip_if_not_express)]
async fn append_object_4(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaa";
    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());

    // now append "bbbb" to the beginning of the object
    let offset_bytes = 0; // byte 0, thus the beginning of the file
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket, &object, data1, offset_bytes)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), size);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket, &object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size().unwrap(), size);

    // retrieve the content of the object and check that it is "aaaa"
    let content: String = String::from_utf8(
        resp.content()
            .unwrap()
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);
}

/// Append beyond the size of a non-existing object (error NoSuchKey)
#[minio_macros::test(skip_if_not_express)]
async fn append_object_5(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaa";
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());

    let offset_bytes = 1; // byte 1, thus beyond the current length of the (non-existing) file
    let resp: Result<AppendObjectResponse, Error> = ctx
        .client
        .append_object(&bucket, &object, data1, offset_bytes)
        .unwrap()
        .build()
        .send()
        .await;

    match resp {
        Ok(v) => panic!("append object should have failed; got value: {v:?}"),
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NoSuchKey);
        }
        Err(e) => panic!("append object should have failed; got error: {e:?}"),
    }
}

#[minio_macros::test(skip_if_not_express)]
async fn append_object_content_0(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let content1 = "aaaaa";
    let content2 = "bbbbb";
    let size = content1.len() as u64;

    create_object_helper(content1, &bucket, &object, &ctx).await;

    let resp: AppendObjectResponse = ctx
        .client
        .append_object_content(&bucket, &object, content2)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), size * 2);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket, &object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size().unwrap(), size * 2);

    let content: String = String::from_utf8(
        resp.content()
            .unwrap()
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, format!("{content1}{content2}"));
}

#[minio_macros::test(skip_if_not_express)]
async fn append_object_content_1(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let n_parts = 3;
    let part_size = 5 * 1024 * 1024;
    let data1: ObjectContent =
        ObjectContent::new_from_stream(RandSrc::new(part_size), Some(part_size));

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket, &object, data1)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), part_size);

    for i in 1..n_parts {
        let expected_size: u64 = (i + 1) * part_size;
        let data2: ObjectContent =
            ObjectContent::new_from_stream(RandSrc::new(part_size), Some(part_size));
        let resp: AppendObjectResponse = ctx
            .client
            .append_object_content(&bucket, &object, data2)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket(), Some(&bucket));
        assert_eq!(resp.object(), Some(&object));
        assert_eq!(resp.object_size(), expected_size);

        let resp: StatObjectResponse = ctx
            .client
            .stat_object(&bucket, &object)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket(), Some(&bucket));
        assert_eq!(resp.object(), Some(&object));
        assert_eq!(resp.size().unwrap(), expected_size);
    }
}

#[minio_macros::test(skip_if_not_express)]
async fn append_object_content_2(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let sizes = [16_u64, 5 * 1024 * 1024, 16 + 5 * 1024 * 1024];

    for size in sizes.iter() {
        let data1: ObjectContent = ObjectContent::new_from_stream(RandSrc::new(*size), Some(*size));

        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket, &object, data1)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket(), Some(&bucket));
        assert_eq!(resp.object(), Some(&object));
        assert_eq!(resp.object_size(), *size);

        let expected_size: u64 = 2 * (*size);
        let data2: ObjectContent = ObjectContent::new_from_stream(RandSrc::new(*size), Some(*size));
        let resp: AppendObjectResponse = ctx
            .client
            .append_object_content(&bucket, &object, data2)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket(), Some(&bucket));
        assert_eq!(resp.object(), Some(&object));
        assert_eq!(resp.object_size(), expected_size);

        let resp: StatObjectResponse = ctx
            .client
            .stat_object(&bucket, &object)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket(), Some(&bucket));
        assert_eq!(resp.object(), Some(&object));
        assert_eq!(resp.size().unwrap(), expected_size);
    }
}

/// Test sending AppendObject across async tasks.
#[minio_macros::test(skip_if_not_express)]
async fn append_object_content_3(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();
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
        let object = object.clone();
        let client = ctx.client.clone();
        let bucket = bucket.clone();
        tokio::spawn(async move {
            let mut idx = 0;
            while let Some(item) = receiver.recv().await {
                let content = "some initial content";
                let initial_size = content.len() as u64;

                let resp: PutObjectContentResponse = client
                    .put_object_content(&bucket, &object, content)
                    .unwrap()
                    .build()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.object_size(), initial_size);

                let resp: AppendObjectResponse = client
                    .append_object_content(&bucket, &object, item)
                    .unwrap()
                    .build()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.object_size(), sizes[idx] + initial_size);
                let etag: ETag = resp.etag().unwrap();

                let resp: StatObjectResponse = client
                    .stat_object(&bucket, &object)
                    .unwrap()
                    .build()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.size().unwrap(), sizes[idx] + initial_size);
                assert_eq!(resp.etag().unwrap(), etag);
                client
                    .delete_object(&bucket, &object)
                    .unwrap()
                    .build()
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

/// Regression test: content that is an exact multiple of MIN_PART_SIZE triggers multipart append
/// where every part fills the buffer completely. The old code exited the loop without setting
/// last_resp, causing a panic. Verifies the fix returns Ok instead of panicking.
#[minio_macros::test(skip_if_not_express)]
async fn append_object_content_exact_part_size_multiple(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();

    let initial_content = "initial";
    let initial_size = initial_content.len() as u64;

    create_object_helper(initial_content, &bucket, &object, &ctx).await;

    // 2 * MIN_PART_SIZE: both parts are exactly full-sized, exercising the case
    // where buffer_size == part_size on every iteration and last_resp was never set.
    let append_size = 2 * MIN_PART_SIZE;
    let data: ObjectContent =
        ObjectContent::new_from_stream(RandSrc::new(append_size), Some(append_size));
    let resp: AppendObjectResponse = ctx
        .client
        .append_object_content(&bucket, &object, data)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), initial_size + append_size);

    let resp: StatObjectResponse = ctx
        .client
        .stat_object(&bucket, &object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.size().unwrap(), initial_size + append_size);
}
