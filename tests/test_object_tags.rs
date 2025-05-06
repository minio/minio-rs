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
use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{
    DeleteObjectTaggingResponse, GetObjectTaggingResponse, PutObjectContentResponse,
    PutObjectTaggingResponse,
};
use minio::s3::types::S3Api;
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_tags() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express() {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let size = 16_u64;
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    let resp: PutObjectTaggingResponse = ctx
        .client
        .put_object_tagging(&bucket_name, &object_name)
        .tags(tags.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectTaggingResponse = ctx
        .client
        .get_object_tagging(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags, tags);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: DeleteObjectTaggingResponse = ctx
        .client
        .delete_object_tagging(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectTaggingResponse = ctx
        .client
        .get_object_tagging(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    assert!(resp.tags.is_empty());
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);
}
