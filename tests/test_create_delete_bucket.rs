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

use crate::common::{TestContext, rand_bucket_name};
use minio::s3::args::{BucketExistsArgs, MakeBucketArgs, RemoveBucketArgs};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn create_delete_bucket() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    ctx.client
        .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let exists = ctx
        .client
        .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(exists);

    ctx.client
        .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let exists = ctx
        .client
        .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(!exists);
}
