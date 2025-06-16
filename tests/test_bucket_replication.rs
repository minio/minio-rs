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

use minio::s3::builders::VersioningStatus;
use minio::s3::client::DEFAULT_REGION;
use minio::s3::error::{Error, ErrorCode};
use minio::s3::response::{
    DeleteBucketReplicationResponse, GetBucketReplicationResponse, GetBucketVersioningResponse,
    PutBucketPolicyResponse, PutBucketReplicationResponse, PutBucketVersioningResponse,
};
use minio::s3::types::{ReplicationConfig, S3Api};
use minio_common::example::{
    create_bucket_policy_config_example_for_replication, create_bucket_replication_config_example,
};
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn bucket_replication_s3() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express().await {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let ctx2 = TestContext::new_from_env();
    let (bucket_name2, _cleanup2) = ctx2.create_bucket_helper().await;

    {
        let resp: PutBucketVersioningResponse = ctx
            .client
            .put_bucket_versioning(&bucket_name)
            .versioning_status(VersioningStatus::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.region, DEFAULT_REGION);

        let resp: PutBucketVersioningResponse = ctx
            .client
            .put_bucket_versioning(&bucket_name2)
            .versioning_status(VersioningStatus::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket, bucket_name2);
        assert_eq!(resp.region, DEFAULT_REGION);

        let resp: GetBucketVersioningResponse = ctx
            .client
            .get_bucket_versioning(&bucket_name)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status, Some(VersioningStatus::Enabled));
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.region, DEFAULT_REGION);

        if false {
            //TODO: to allow replication policy needs to be applied, but this fails
            let config: String = create_bucket_policy_config_example_for_replication();
            let _resp: PutBucketPolicyResponse = ctx
                .client
                .put_bucket_policy(&bucket_name)
                .config(config.clone())
                .send()
                .await
                .unwrap();

            let _resp: PutBucketPolicyResponse = ctx
                .client
                .put_bucket_policy(&bucket_name2)
                .config(config.clone())
                .send()
                .await
                .unwrap();
        }
    }

    if false {
        let config: ReplicationConfig = create_bucket_replication_config_example(&bucket_name2);

        //TODO setup permissions that allow replication
        // TODO panic: called `Result::unwrap()` on an `Err` value: S3Error(ErrorResponse { code: "XMinioAdminRemoteTargetNotFoundError", message: "The remote target does not exist",
        let resp: PutBucketReplicationResponse = ctx
            .client
            .put_bucket_replication(&bucket_name)
            .replication_config(config.clone())
            .send()
            .await
            .unwrap();
        //println!("response of setting replication: resp={:?}", resp);
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.region, DEFAULT_REGION);

        let resp: GetBucketReplicationResponse = ctx
            .client
            .get_bucket_replication(&bucket_name)
            .send()
            .await
            .unwrap();
        //assert_eq!(resp.config, config); //TODO
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.region, DEFAULT_REGION);

        // TODO called `Result::unwrap()` on an `Err` value: S3Error(ErrorResponse { code: "XMinioAdminRemoteTargetNotFoundError", message: "The remote target does not exist",
        let resp: DeleteBucketReplicationResponse = ctx
            .client
            .delete_bucket_replication(&bucket_name)
            .send()
            .await
            .unwrap();
        println!("response of deleting replication: resp={:?}", resp);
    }
    let _resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    //println!("response of getting replication: resp={:?}", resp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn bucket_replication_s3express() {
    let ctx = TestContext::new_from_env();

    if !ctx.client.is_minio_express().await {
        println!("Skipping test because it is NOT running in MinIO Express mode");
        return;
    }
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let config: ReplicationConfig = create_bucket_replication_config_example(&bucket_name);

    let resp: Result<PutBucketReplicationResponse, Error> = ctx
        .client
        .put_bucket_replication(&bucket_name)
        .replication_config(config.clone())
        .send()
        .await;
    match resp {
        Err(Error::S3Error(e)) => assert_eq!(e.code, ErrorCode::NotSupported),
        v => panic!("Expected error S3Error(NotSupported): but got {:?}", v),
    }

    let resp: Result<GetBucketReplicationResponse, Error> =
        ctx.client.get_bucket_replication(&bucket_name).send().await;
    match resp {
        Err(Error::S3Error(e)) => assert_eq!(e.code, ErrorCode::NotSupported),
        v => panic!("Expected error S3Error(NotSupported): but got {:?}", v),
    }

    let resp: Result<DeleteBucketReplicationResponse, Error> = ctx
        .client
        .delete_bucket_replication(&bucket_name)
        .send()
        .await;
    match resp {
        Err(Error::S3Error(e)) => assert_eq!(e.code, ErrorCode::NotSupported),
        v => panic!("Expected error S3Error(NotSupported): but got {:?}", v),
    }
}
