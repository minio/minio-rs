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
use minio::s3::response_traits::{HasBucket, HasObject, HasRegion, HasTagging, HasVersion};
use minio::s3::types::{BucketName, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use std::collections::HashMap;

#[minio_macros::test(skip_if_express)]
async fn object_tags(ctx: TestContext, bucket_name: BucketName) {
    let object_name = rand_object_name();

    let size = 16_u64;
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            bucket_name.clone(),
            object_name.clone(),
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());
    assert_eq!(resp.object_size(), size);
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION.as_str());

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    let resp: PutObjectTaggingResponse = ctx
        .client
        .put_object_tagging(bucket_name.clone(), object_name.clone())
        .tags(tags.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION.as_str());

    let resp: GetObjectTaggingResponse = ctx
        .client
        .get_object_tagging(bucket_name.clone(), object_name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().unwrap(), tags);
    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION.as_str());

    let resp: DeleteObjectTaggingResponse = ctx
        .client
        .delete_object_tagging(bucket_name.clone(), object_name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION.as_str());

    let resp: GetObjectTaggingResponse = ctx
        .client
        .get_object_tagging(bucket_name.clone(), object_name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.tags().unwrap().is_empty());
    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION.as_str());
}
