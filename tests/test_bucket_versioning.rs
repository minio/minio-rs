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

use crate::common::{TestContext, create_bucket_helper};
use minio::s3::builders::VersioningStatus;
use minio::s3::response::{GetBucketVersioningResponse, SetBucketVersioningResponse};
use minio::s3::types::S3Api;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_versioning() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let _resp: SetBucketVersioningResponse = ctx
        .client
        .set_bucket_versioning(&bucket_name)
        .versioning_status(VersioningStatus::Enabled)
        .send()
        .await
        .unwrap();

    let resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status, Some(VersioningStatus::Enabled));

    let _resp: SetBucketVersioningResponse = ctx
        .client
        .set_bucket_versioning(&bucket_name)
        .versioning_status(VersioningStatus::Suspended)
        .send()
        .await
        .unwrap();

    let resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status, Some(VersioningStatus::Suspended));
}
