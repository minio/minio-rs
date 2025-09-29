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

use minio::s3::client::DEFAULT_REGION;
use minio::s3::error::{Error, S3ServerError};
use minio::s3::minio_error_response::MinioErrorCode;
use minio::s3::response::a_response_traits::{HasBucket, HasObject, HasRegion};
use minio::s3::response::{
    BucketExistsResponse, CreateBucketResponse, DeleteBucketResponse, PutObjectContentResponse,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use minio_common::utils::{rand_bucket_name, rand_object_name_utf8};

#[minio_macros::test(no_bucket)]
async fn bucket_create(ctx: TestContext) {
    let bucket_name = rand_bucket_name();

    // try to create a bucket that does not exist
    let resp: CreateBucketResponse = ctx
        .client
        .create_bucket(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    // check that the bucket exists
    let resp: BucketExistsResponse = ctx
        .client
        .bucket_exists(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.exists());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    // try to create a bucket that already exists
    let resp: Result<CreateBucketResponse, Error> =
        ctx.client.create_bucket(&bucket_name).build().send().await;
    match resp {
        Ok(_) => panic!("Bucket already exists, but was created again"),
        Err(Error::S3Server(S3ServerError::S3Error(e)))
            if matches!(e.code(), MinioErrorCode::BucketAlreadyOwnedByYou) =>
        {
            // this is expected, as the bucket already exists
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
}

#[minio_macros::test(no_bucket)]
async fn bucket_delete(ctx: TestContext) {
    let bucket_name = rand_bucket_name();

    // try to remove a bucket that does not exist
    let resp: Result<DeleteBucketResponse, Error> =
        ctx.client.delete_bucket(&bucket_name).build().send().await;
    match resp {
        Ok(_) => panic!("Bucket does not exist, but was removed"),
        Err(Error::S3Server(S3ServerError::S3Error(e)))
            if matches!(e.code(), MinioErrorCode::NoSuchBucket) =>
        {
            // this is expected, as the bucket does not exist
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // create a new bucket
    let resp: CreateBucketResponse = ctx
        .client
        .create_bucket(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    // check that the bucket exists
    let resp: BucketExistsResponse = ctx
        .client
        .bucket_exists(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.exists());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    // try to remove a bucket that exists
    let resp: DeleteBucketResponse = ctx
        .client
        .delete_bucket(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    // check that the bucket does not exist anymore
    let resp: BucketExistsResponse = ctx
        .client
        .bucket_exists(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(!resp.exists());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), "");
}

async fn test_bucket_delete_and_purge(ctx: &TestContext, bucket_name: &str, object_name: &str) {
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(bucket_name, object_name, "Hello, World!")
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);

    // try to remove the bucket without purging, this should fail because the bucket is not empty
    let resp: Result<DeleteBucketResponse, Error> =
        ctx.client.delete_bucket(bucket_name).build().send().await;

    assert!(resp.is_err());

    // try to remove the bucket with purging, this should succeed
    let resp: DeleteBucketResponse = ctx
        .client
        .delete_and_purge_bucket(bucket_name)
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
}

/// Test purging a bucket with an object that contains utf8 characters.
#[minio_macros::test]
async fn bucket_delete_and_purge_1(ctx: TestContext, bucket_name: String) {
    test_bucket_delete_and_purge(&ctx, &bucket_name, &rand_object_name_utf8(20)).await;
}

/// Test purging a bucket with an object that contains white space characters.
#[minio_macros::test]
async fn bucket_delete_and_purge_2(ctx: TestContext, bucket_name: String) {
    test_bucket_delete_and_purge(&ctx, &bucket_name, "a b+c").await;
}
