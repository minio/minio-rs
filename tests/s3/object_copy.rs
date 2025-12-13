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

use minio::s3::builders::{CopySource, ObjectContent};
use minio::s3::response::{CopyObjectResponse, PutObjectContentResponse, StatObjectResponse};
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name_utf8;

async fn test_copy_object(
    ctx: &TestContext,
    bucket_name: &str,
    object_name_src: &str,
    object_name_dst: &str,
) {
    let size = 16_u64;
    let content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            BucketName::try_from(bucket_name).unwrap(),
            ObjectKey::try_from(object_name_src).unwrap(),
            content,
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name_src);

    let resp: CopyObjectResponse = ctx
        .client
        .copy_object(
            BucketName::try_from(bucket_name).unwrap(),
            ObjectKey::try_from(object_name_dst).unwrap(),
        )
        .source(
            CopySource::builder()
                .bucket(BucketName::try_from(bucket_name).unwrap())
                .object(ObjectKey::try_from(object_name_src).unwrap())
                .build(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name_dst);

    let resp: StatObjectResponse = ctx
        .client
        .stat_object(
            BucketName::try_from(bucket_name).unwrap(),
            ObjectKey::try_from(object_name_dst).unwrap(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.size().unwrap(), size);
    assert_eq!(resp.bucket(), bucket_name);
}

/// Test copying an object with a name that contains utf8 characters.
#[minio_macros::test(skip_if_express)]
async fn copy_object_1(ctx: TestContext, bucket_name: BucketName) {
    let src_name = rand_object_name_utf8(20);
    let dst_name = rand_object_name_utf8(20);
    test_copy_object(
        &ctx,
        bucket_name.as_str(),
        src_name.as_str(),
        dst_name.as_str(),
    )
    .await;
}

/// Test copying an object with a name that contains white space characters.
#[minio_macros::test(skip_if_express)]
async fn copy_object_2(ctx: TestContext, bucket_name: BucketName) {
    test_copy_object(&ctx, bucket_name.as_str(), "a b+c", "a b+c2").await;
}
