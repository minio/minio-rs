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

use crate::common::{create_bucket_helper, TestContext};
use minio::s3::args::{DeleteBucketTagsArgs, GetBucketTagsArgs, SetBucketTagsArgs};
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_tags() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    ctx.client
        .set_bucket_tags(&SetBucketTagsArgs::new(&bucket_name, &tags).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_tags(&GetBucketTagsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.len() == tags.len() && resp.tags.keys().all(|k| tags.contains_key(k)));

    ctx.client
        .delete_bucket_tags(&DeleteBucketTagsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_tags(&GetBucketTagsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.is_empty());
}
