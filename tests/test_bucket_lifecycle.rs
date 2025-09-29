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
use minio::s3::lifecycle_config::LifecycleConfig;
use minio::s3::minio_error_response::MinioErrorCode;
use minio::s3::response::a_response_traits::{HasBucket, HasRegion};
use minio::s3::response::{
    DeleteBucketLifecycleResponse, GetBucketLifecycleResponse, PutBucketLifecycleResponse,
};
use minio::s3::types::S3Api;
use minio_common::example::create_bucket_lifecycle_config_examples;
use minio_common::test_context::TestContext;

#[minio_macros::test]
async fn bucket_lifecycle(ctx: TestContext, bucket_name: String) {
    let config: LifecycleConfig = create_bucket_lifecycle_config_examples();

    let resp: PutBucketLifecycleResponse = ctx
        .client
        .put_bucket_lifecycle(&bucket_name)
        .life_cycle_config(config.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetBucketLifecycleResponse = ctx
        .client
        .get_bucket_lifecycle(&bucket_name)
        .with_updated_at(false)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.config().unwrap(), config);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert!(resp.updated_at().is_none());

    let resp: GetBucketLifecycleResponse = ctx
        .client
        .get_bucket_lifecycle(&bucket_name)
        .with_updated_at(true)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.config().unwrap(), config);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert!(resp.updated_at().is_some());

    let resp: DeleteBucketLifecycleResponse = ctx
        .client
        .delete_bucket_lifecycle(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: Result<GetBucketLifecycleResponse, Error> = ctx
        .client
        .get_bucket_lifecycle(&bucket_name)
        .build()
        .send()
        .await;
    match resp {
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NoSuchLifecycleConfiguration)
        }
        v => panic!("Expected error S3Error(NoSuchLifecycleConfiguration): but got {v:?}"),
    }
}
