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
use minio::s3::response::a_response_traits::{HasBucket, HasRegion};
use minio::s3::response::{BucketExistsResponse, DeleteBucketResponse};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn bucket_exists(ctx: TestContext, bucket_name: String) {
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

    let resp: DeleteBucketResponse = ctx
        .client
        .delete_bucket(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

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
