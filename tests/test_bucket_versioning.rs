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

use minio::s3::builders::VersioningStatus;
use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{GetBucketVersioningResponse, SetBucketVersioningResponse};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use test_tag::tag;

#[tag(s3)]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_bucket_versioning() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let resp: SetBucketVersioningResponse = ctx
        .client
        .set_bucket_versioning(&bucket_name)
        .versioning_status(VersioningStatus::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status, Some(VersioningStatus::Enabled));
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: SetBucketVersioningResponse = ctx
        .client
        .set_bucket_versioning(&bucket_name)
        .versioning_status(VersioningStatus::Suspended)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status, Some(VersioningStatus::Suspended));
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
}
