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
use minio::s3::error::Error;
use minio::s3::response::{CreateBucketResponse, DeleteBucketResponse};
use minio::s3::response_traits::{HasBucket, HasRegion};
use minio::s3::types::{BucketName, S3Api};
use minio::s3inventory::{
    DeleteInventoryConfigResponse, DestinationSpec, Field, GetInventoryConfigResponse,
    JobDefinition, ModeSpec, OnOrOff, OutputFormat, PutInventoryConfigResponse, Schedule,
    VersionsSpec,
};
use minio_common::test_context::TestContext;

#[minio_macros::test]
async fn delete_inventory_config(ctx: TestContext, bucket_name: BucketName) {
    let job_id = "test-delete-job";
    let dest_bucket_str = format!("{bucket_name}-dest");
    let bucket = bucket_name.clone();
    let dest_bucket = BucketName::new(&dest_bucket_str).unwrap();

    let resp: CreateBucketResponse = ctx
        .client
        .create_bucket(&dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), dest_bucket_str);
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Create inventory job
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket_str.clone(),
            prefix: Some("reports/".to_string()),
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Once,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![Field::ETag, Field::StorageClass],
        filters: None,
    };

    let resp: PutInventoryConfigResponse = ctx
        .client
        .put_inventory_config(&bucket, job_id, job)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap(), &bucket_name);
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Verify job exists
    let resp: GetInventoryConfigResponse = ctx
        .client
        .get_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap(), &bucket_name);
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Delete inventory config
    let resp: DeleteInventoryConfigResponse = ctx
        .client
        .delete_inventory_config(&bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap(), &bucket_name);
    assert_eq!(resp.region(), &*DEFAULT_REGION);

    // Verify job no longer exists
    let resp: Result<GetInventoryConfigResponse, Error> = ctx
        .client
        .get_inventory_config(bucket, job_id)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Job should not exist after deletion");

    // Cleanup
    let resp: DeleteBucketResponse = ctx
        .client
        .delete_bucket(dest_bucket)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket().unwrap().as_str(), dest_bucket_str);
    assert_eq!(resp.region(), &*DEFAULT_REGION);
}
