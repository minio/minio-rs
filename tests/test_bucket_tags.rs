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
use minio::s3::response::{DeleteBucketTagsResponse, GetBucketTagsResponse, SetBucketTagsResponse};
use minio::s3::types::S3Api;
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_tags() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    let _resp: SetBucketTagsResponse = ctx
        .client
        .set_bucket_tags(&bucket_name)
        .tags(tags.clone())
        .send()
        .await
        .unwrap();

    let resp: GetBucketTagsResponse = ctx
        .client
        .get_bucket_tags(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags, resp.tags);

    let _resp: DeleteBucketTagsResponse = ctx
        .client
        .delete_bucket_tags(&bucket_name)
        .send()
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_tags(&bucket_name)
        .send()
        .await
        .unwrap();
    assert!(resp.tags.is_empty());
}
