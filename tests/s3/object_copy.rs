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
    bucket: BucketName,
    object_src: ObjectKey,
    object_dst: ObjectKey,
) {
    let size = 16_u64;
    let content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket, &object_src, content)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object_src));

    let resp: CopyObjectResponse = ctx
        .client
        .copy_object(&bucket, &object_dst)
        .unwrap()
        .source(
            CopySource::builder()
                .bucket(&bucket)
                .object(&object_src)
                .build(),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object_dst));

    let resp: StatObjectResponse = ctx
        .client
        .stat_object(&bucket, &object_dst)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.size().unwrap(), size);
    assert_eq!(resp.bucket(), Some(&bucket));
}

/// Test copying an object with a name that contains utf8 characters.
#[minio_macros::test(skip_if_express)]
async fn copy_object_1(ctx: TestContext, bucket: BucketName) {
    let src_name = rand_object_name_utf8(20);
    let dst_name = rand_object_name_utf8(20);
    test_copy_object(&ctx, bucket, src_name, dst_name).await;
}

/// Test copying an object with a name that contains white space characters.
#[minio_macros::test(skip_if_express)]
async fn copy_object_2(ctx: TestContext, bucket: BucketName) {
    test_copy_object(
        &ctx,
        bucket,
        ObjectKey::try_from("a b+c").unwrap(),
        ObjectKey::try_from("a b+c2").unwrap(),
    )
    .await;
}
