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
use minio::s3::error::{Error, ErrorCode};
use minio::s3::response::{BucketExistsResponse, MakeBucketResponse, RemoveBucketResponse};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_bucket_name;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn bucket_create() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    // try to create a bucket that does not exist
    let resp: MakeBucketResponse = ctx.client.make_bucket(&bucket_name).send().await.unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    // check that the bucket exists
    let resp: BucketExistsResponse = ctx.client.bucket_exists(&bucket_name).send().await.unwrap();
    assert!(resp.exists);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    // try to create a bucket that already exists
    let resp: Result<MakeBucketResponse, Error> = ctx.client.make_bucket(&bucket_name).send().await;
    match resp {
        Ok(_) => panic!("Bucket already exists, but was created again"),
        Err(Error::S3Error(e)) if e.code == ErrorCode::BucketAlreadyOwnedByYou => {
            // this is expected, as the bucket already exists
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn bucket_delete() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    // try to remove a bucket that does not exist
    let resp: Result<RemoveBucketResponse, Error> =
        ctx.client.remove_bucket(&bucket_name).send().await;
    match resp {
        Ok(_) => panic!("Bucket does not exist, but was removed"),
        Err(Error::S3Error(e)) if e.code == ErrorCode::NoSuchBucket => {
            // this is expected, as the bucket does not exist
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }

    // create a new bucket
    let resp: MakeBucketResponse = ctx.client.make_bucket(&bucket_name).send().await.unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    // check that the bucket exists
    let resp: BucketExistsResponse = ctx.client.bucket_exists(&bucket_name).send().await.unwrap();
    assert!(resp.exists);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    // try to remove a bucket that exists
    let resp: RemoveBucketResponse = ctx.client.remove_bucket(&bucket_name).send().await.unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    // check that the bucket does not exist anymore
    let resp: BucketExistsResponse = ctx.client.bucket_exists(&bucket_name).send().await.unwrap();
    assert!(!resp.exists);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, "");
}
