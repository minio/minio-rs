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
use minio::s3::response::a_response_traits::{HasBucket, HasObject};
use minio::s3::response::{CopyObjectResponse, PutObjectContentResponse, StatObjectResponse};
use minio::s3::types::S3Api;
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
        .put_object_content(bucket_name, object_name_src, content)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name_src);

    let resp: CopyObjectResponse = ctx
        .client
        .copy_object(bucket_name, object_name_dst)
        .source(
            CopySource::builder()
                .bucket(bucket_name)
                .object(object_name_src)
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
        .stat_object(bucket_name, object_name_dst)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.size().unwrap(), size);
    assert_eq!(resp.bucket(), bucket_name);
}

/// Test copying an object with a name that contains utf8 characters.
#[minio_macros::test(skip_if_express)]
async fn copy_object_1(ctx: TestContext, bucket_name: String) {
    test_copy_object(
        &ctx,
        &bucket_name,
        &rand_object_name_utf8(20),
        &rand_object_name_utf8(20),
    )
    .await;
}

/// Test copying an object with a name that contains white space characters.
#[minio_macros::test(skip_if_express)]
async fn copy_object_2(ctx: TestContext, bucket_name: String) {
    test_copy_object(&ctx, &bucket_name, "a b+c", "a b+c2").await;
}
