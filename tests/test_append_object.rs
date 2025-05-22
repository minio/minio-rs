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

use minio::s3::builders::ObjectContent;
use minio::s3::error::{Error, ErrorCode};
use minio::s3::response::{
    AppendObjectResponse, GetObjectResponse, PutObjectContentResponse, PutObjectResponse,
    StatObjectResponse,
};
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use tokio::sync::mpsc;

/// Append to the end of an existing object (happy flow)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_0() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";

    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());

    // first create an object (with put) that contains "aaaa"
    let resp: PutObjectResponse = ctx
        .client
        .put_object(&bucket_name, &object_name, data1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    // double check that the content we just have put is "aaaa"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);

    // now append "bbbb" to the end of the object
    let offset_bytes = size;
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket_name, &object_name, data2, offset_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size * 2);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();

    // retrieve the content of the object and check that it is "aaaabbbb"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, format!("{}{}", content1, content2));

    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size * 2);
}

/// Append to the beginning of an existing object (happy flow)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_1() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";

    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());

    // first create an object (with put) that contains "aaaa"
    let resp: PutObjectResponse = ctx
        .client
        .put_object(&bucket_name, &object_name, data1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    // double check that the content we just have put is "aaaa"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);

    // now append "bbbb" to the beginning of the object
    let offset_bytes = 0; // byte 0, thus the beginning of the file
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket_name, &object_name, data2, offset_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();

    // retrieve the content of the object and check that it is "bbbb"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content2);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);
}

/// Append to the middle of an existing object (error InvalidWriteOffset)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_2() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";

    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());

    // first create an object (with put) that contains "aaaa"
    let resp: PutObjectResponse = ctx
        .client
        .put_object(&bucket_name, &object_name, data1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    // double check that the content we just have put is "aaaa"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);

    // now try to append "bbbb" to the object at an invalid offset
    let offset_bytes = size - 1;
    let resp: Result<AppendObjectResponse, Error> = ctx
        .client
        .append_object(&bucket_name, &object_name, data2, offset_bytes)
        .send()
        .await;

    match resp {
        Ok(v) => panic!("append object should have failed; got value: {:?}", v),
        Err(Error::S3Error(e)) => {
            assert_eq!(e.code, ErrorCode::InvalidWriteOffset);
        }
        Err(e) => panic!("append object should have failed; got error: {:?}", e),
    }
}

/// Append beyond the size of an existing object (error InvalidWriteOffset)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_3() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaa";
    let content2 = "bbbb";

    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());
    let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());

    // first create an object (with put) that contains "aaaa"
    let resp: PutObjectResponse = ctx
        .client
        .put_object(&bucket_name, &object_name, data1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    // double check that the content we just have put is "aaaa"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);

    // now try to append "bbbb" to the object at an invalid offset
    let offset_bytes = size + 1;
    let resp: Result<AppendObjectResponse, Error> = ctx
        .client
        .append_object(&bucket_name, &object_name, data2, offset_bytes)
        .send()
        .await;

    match resp {
        Ok(v) => panic!("append object should have failed; got value: {:?}", v),
        Err(Error::S3Error(e)) => {
            assert_eq!(e.code, ErrorCode::InvalidWriteOffset);
        }
        Err(e) => panic!("append object should have failed; got error: {:?}", e),
    }
}

/// Append to the beginning/end of a non-existing object (happy flow)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_4() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaa";
    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());

    // now append "bbbb" to the beginning of the object
    let offset_bytes = 0; // byte 0, thus the beginning of the file
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket_name, &object_name, data1, offset_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();

    // retrieve the content of the object and check that it is "aaaa"
    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);
}

/// Append beyond the size of a non-existing object (error NoSuchKey)
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_5() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaa";
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());

    let offset_bytes = 1; // byte 1, thus beyond the current length of the (non-existing) file
    let resp: Result<AppendObjectResponse, Error> = ctx
        .client
        .append_object(&bucket_name, &object_name, data1, offset_bytes)
        .send()
        .await;

    match resp {
        Ok(v) => panic!("append object should have failed; got value: {:?}", v),
        Err(Error::S3Error(e)) => {
            assert_eq!(e.code, ErrorCode::NoSuchKey);
        }
        Err(e) => panic!("append object should have failed; got error: {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_content_0() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let content1 = "aaaaa";
    let content2 = "bbbbb";

    let size = content1.len() as u64;
    let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());
    //let data2: SegmentedBytes = SegmentedBytes::from(content2.to_string());

    let resp: PutObjectResponse = ctx
        .client
        .put_object(&bucket_name, &object_name, data1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);

    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, content1);

    let resp: AppendObjectResponse = ctx
        .client
        .append_object_content(&bucket_name, &object_name, content2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size * 2);

    let resp: GetObjectResponse = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();

    let content: String = String::from_utf8(
        resp.content
            .to_segmented_bytes()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(content, format!("{}{}", content1, content2));

    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size * 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_content_1() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let n_parts = 3;
    let part_size = 5 * 1024 * 1024;
    let data1: ObjectContent =
        ObjectContent::new_from_stream(RandSrc::new(part_size), Some(part_size));

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, data1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, part_size);

    for i in 1..n_parts {
        let expected_size: u64 = (i + 1) * part_size;
        let data2: ObjectContent =
            ObjectContent::new_from_stream(RandSrc::new(part_size), Some(part_size));
        let resp: AppendObjectResponse = ctx
            .client
            .append_object_content(&bucket_name, &object_name, data2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.object, object_name);
        assert_eq!(resp.object_size, expected_size);

        let resp: StatObjectResponse = ctx
            .client
            .stat_object(&bucket_name, &object_name)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.object, object_name);
        assert_eq!(resp.size, expected_size);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_content_2() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let sizes = [16_u64, 5 * 1024 * 1024, 16 + 5 * 1024 * 1024];

    for size in sizes.iter() {
        let data1: ObjectContent = ObjectContent::new_from_stream(RandSrc::new(*size), Some(*size));

        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &object_name, data1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.object, object_name);
        assert_eq!(resp.object_size, *size);

        let expected_size: u64 = 2 * (*size);
        let data2: ObjectContent = ObjectContent::new_from_stream(RandSrc::new(*size), Some(*size));
        let resp: AppendObjectResponse = ctx
            .client
            .append_object_content(&bucket_name, &object_name, data2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.object, object_name);
        assert_eq!(resp.object_size, expected_size);

        let resp: StatObjectResponse = ctx
            .client
            .stat_object(&bucket_name, &object_name)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.object, object_name);
        assert_eq!(resp.size, expected_size);
    }
}

/// Test sending AppendObject across async tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn append_object_content_3() {
    let ctx = TestContext::new_from_env();
    if !ctx.client.is_minio_express() {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
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
                let content = "some initial content";
                let initial_size = content.len() as u64;

                let resp: PutObjectContentResponse = client
                    .put_object_content(&test_bucket, &object_name, content)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.object_size, initial_size);

                let resp: AppendObjectResponse = client
                    .append_object_content(&test_bucket, &object_name, item)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.object_size, sizes[idx] + initial_size);
                let etag = resp.etag;

                let resp: StatObjectResponse = client
                    .stat_object(&test_bucket, &object_name)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.size, sizes[idx] + initial_size);
                assert_eq!(resp.etag, etag);
                client
                    .remove_object(&test_bucket, &object_name)
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
