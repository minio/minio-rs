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

use http::Method;
use minio::s3::client::DEFAULT_REGION;
use minio::s3::header_constants::*;
use minio::s3::response::GetPresignedObjectUrlResponse;
use minio::s3::types::BucketName;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

#[minio_macros::test]
async fn get_presigned_object_url(ctx: TestContext, bucket_name: BucketName) {
    let object_name = rand_object_name();
    let resp: GetPresignedObjectUrlResponse = ctx
        .client
        .get_presigned_object_url(bucket_name.clone(), object_name.clone(), Method::GET)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.url.contains(X_AMZ_SIGNATURE));
    assert_eq!(resp.bucket.as_str(), bucket_name.as_str());
    assert_eq!(resp.object.as_str(), object_name.as_str());
    assert_eq!(resp.region.as_str(), DEFAULT_REGION.as_str());
}
