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
use minio::s3::response::a_response_traits::{HasBucket, HasObject, HasRegion, HasVersion};
use minio::s3::response::{
    GetObjectRetentionResponse, PutObjectContentResponse, PutObjectRetentionResponse,
};
use minio::s3::types::{RetentionMode, S3Api};
use minio::s3::utils::{to_iso8601utc, utc_now};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

#[minio_macros::test(skip_if_express, object_lock)]
async fn object_retention(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();

    let size = 16_u64;

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), size);
    assert_ne!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION);
    //assert_eq!(resp.etag, "");

    let retain_until_date = utc_now() + chrono::Duration::days(1);
    let resp: PutObjectRetentionResponse = ctx
        .client
        .put_object_retention(&bucket_name, &object_name)
        .retention_mode(RetentionMode::GOVERNANCE)
        .retain_until_date(retain_until_date)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetObjectRetentionResponse = ctx
        .client
        .get_object_retention(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.retention_mode().unwrap().unwrap(),
        RetentionMode::GOVERNANCE
    );
    assert_eq!(
        to_iso8601utc(resp.retain_until_date().unwrap().unwrap()),
        to_iso8601utc(retain_until_date)
    );

    let resp: PutObjectRetentionResponse = ctx
        .client
        .put_object_retention(&bucket_name, &object_name)
        .bypass_governance_mode(true)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION);

    let resp: GetObjectRetentionResponse = ctx
        .client
        .get_object_retention(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.retention_mode().unwrap().is_none());
    assert!(resp.retain_until_date().unwrap().is_none());
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.version_id(), None);
    assert_eq!(resp.region(), DEFAULT_REGION);
}
