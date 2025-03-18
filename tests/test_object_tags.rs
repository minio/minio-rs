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

use crate::common::{TestContext, create_bucket_helper, rand_object_name};
use common::RandSrc;
use minio::s3::builders::ObjectContent;
use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{
    DeleteObjectTagsResponse, GetObjectTagsResponse, PutObjectContentResponse,
    RemoveObjectResponse, SetObjectTagsResponse,
};
use minio::s3::types::S3Api;
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_tags() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
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
    assert_eq!(&resp.location, "");

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    let resp: SetObjectTagsResponse = ctx
        .client
        .set_object_tags(&bucket_name)
        .object(object_name.clone())
        .tags(tags.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectTagsResponse = ctx
        .client
        .get_object_tags(&bucket_name)
        .object(object_name.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags, tags);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: DeleteObjectTagsResponse = ctx
        .client
        .delete_object_tags(&bucket_name)
        .object(object_name.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: GetObjectTagsResponse = ctx
        .client
        .get_object_tags(&bucket_name.clone())
        .object(object_name.clone())
        .send()
        .await
        .unwrap();
    assert!(resp.tags.is_empty());
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.version_id, None);
    assert_eq!(resp.region, DEFAULT_REGION);

    let resp: RemoveObjectResponse = ctx
        .client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.version_id, None);
    assert!(!resp.is_delete_marker)
}
