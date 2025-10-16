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

use minio::s3::client::DEFAULT_REGION;
use minio::s3::error::{Error, S3ServerError};
use minio::s3::minio_error_response::MinioErrorCode;
use minio::s3::response::a_response_traits::{HasBucket, HasRegion, HasTagging};
use minio::s3::response::{
    DeleteBucketTaggingResponse, GetBucketTaggingResponse, PutBucketTaggingResponse,
};
use minio::s3::types::S3Api;
use minio_common::example::create_tags_example;
use minio_common::test_context::TestContext;

#[minio_macros::test(skip_if_express)]
async fn bucket_tags_s3(ctx: TestContext, bucket_name: String) {
    let tags = create_tags_example();

    let resp: PutBucketTaggingResponse = ctx
        .client
        .put_bucket_tagging(&bucket_name)
        .tags(tags.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetBucketTaggingResponse = ctx
        .client
        .get_bucket_tagging(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().unwrap(), tags);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: DeleteBucketTaggingResponse = ctx
        .client
        .delete_bucket_tagging(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetBucketTaggingResponse = ctx
        .client
        .get_bucket_tagging(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.tags().unwrap().is_empty());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
}

#[minio_macros::test(skip_if_not_express)]
async fn bucket_tags_s3express(ctx: TestContext, bucket_name: String) {
    let tags = create_tags_example();

    let resp: Result<PutBucketTaggingResponse, Error> = ctx
        .client
        .put_bucket_tagging(&bucket_name)
        .tags(tags.clone())
        .build()
        .send()
        .await;
    match resp {
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NotSupported)
        }
        v => panic!("Expected error S3Error(NotSupported): but got {v:?}"),
    }

    let resp: Result<GetBucketTaggingResponse, Error> = ctx
        .client
        .get_bucket_tagging(&bucket_name)
        .build()
        .send()
        .await;
    match resp {
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NotSupported)
        }
        v => panic!("Expected error S3Error(NotSupported): but got {v:?}"),
    }

    let resp: Result<DeleteBucketTaggingResponse, Error> = ctx
        .client
        .delete_bucket_tagging(&bucket_name)
        .build()
        .send()
        .await;
    match resp {
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NotSupported)
        }
        v => panic!("Expected error S3Error(NotSupported): but got {v:?}"),
    }
}
