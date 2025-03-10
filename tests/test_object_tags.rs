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

use crate::common::{create_bucket_helper, rand_object_name, RandReader, TestContext};
use minio::s3::args::{DeleteObjectTagsArgs, GetObjectTagsArgs, PutObjectArgs, SetObjectTagsArgs};
use minio::s3::types::S3Api;
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_tags() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    ctx.client
        .set_object_tags(&SetObjectTagsArgs::new(&bucket_name, &object_name, &tags).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_object_tags(&GetObjectTagsArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.len() == tags.len() && resp.tags.keys().all(|k| tags.contains_key(k)));

    ctx.client
        .delete_object_tags(&DeleteObjectTagsArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_object_tags(&GetObjectTagsArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.is_empty());

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}
