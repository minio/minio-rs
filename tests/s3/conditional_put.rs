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
use minio::s3::error::{Error, S3ServerError};
use minio::s3::response::PutObjectContentResponse;
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::BucketName;
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

/// Conditional PUT with `If-None-Match: *` (`not_match_etag("*")`): the write
/// must succeed when the key is fresh and fail with a precondition error once
/// the object already exists. This is standard S3 behavior, so the test is not
/// gated to AIStor.
#[minio_macros::test]
async fn conditional_put_if_none_match(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();
    let size = 16_u64;

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket,
            &object,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .unwrap()
        .not_match_etag("*".to_string())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
    assert_eq!(resp.object_size(), size);

    let resp: Result<PutObjectContentResponse, Error> = ctx
        .client
        .put_object_content(
            &bucket,
            &object,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .unwrap()
        .not_match_etag("*".to_string())
        .build()
        .send()
        .await;

    match resp {
        Ok(v) => panic!("conditional put should have failed; got value: {v:?}"),
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert!(
                e.code().to_string().to_lowercase().contains("precondition"),
                "expected a precondition-failed error, got code: {}",
                e.code()
            );
        }
        Err(e) => panic!("conditional put should have failed with an S3 error; got: {e:?}"),
    }
}
