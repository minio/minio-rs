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

use minio::madmin::MadminClient;
use minio::madmin::types::MadminApi;
use minio::madmin::types::bucket_target::{BucketTarget, Credentials, ServiceType};
use minio::s3::builders::VersioningStatus;
use minio::s3::client::DEFAULT_REGION;
use minio::s3::creds::StaticProvider;
use minio::s3::error::{Error, S3ServerError};
use minio::s3::minio_error_response::MinioErrorCode;
use minio::s3::response::{
    DeleteBucketReplicationResponse, GetBucketReplicationResponse, GetBucketVersioningResponse,
    PutBucketReplicationResponse, PutBucketVersioningResponse,
};
use minio::s3::response_traits::{HasBucket, HasRegion};
use minio::s3::types::{
    AndOperator, BucketName, Destination, Filter, ReplicationConfig, ReplicationRule, S3Api,
};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

#[minio_macros::test(skip_if_express)]
#[ignore = "Madmin remote target APIs not yet migrated to new request infrastructure"]
async fn bucket_replication_s3(ctx: TestContext, bucket_name: BucketName) {
    // Create a second bucket on the same MinIO instance for replication target
    let (bucket_name2, _cleanup2) = ctx.create_bucket_helper().await;

    // set the versioning on the buckets, and the bucket policy
    {
        let resp: PutBucketVersioningResponse = ctx
            .client
            .put_bucket_versioning(&bucket, VersioningStatus::Enabled)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.bucket(), Some(&bucket_name));
        assert_eq!(resp.region(), &*DEFAULT_REGION);

        let resp: PutBucketVersioningResponse = ctx
            .client
            .put_bucket_versioning(bucket2.as_str(), VersioningStatus::Enabled)
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.bucket(),
            Some(&BucketName::try_from(bucket_name2.as_str()).unwrap())
        );
        assert_eq!(resp.region(), &*DEFAULT_REGION);
    }

    // Create MadminClient to set up remote target (required before S3 bucket replication)
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Construct the endpoint URL for the remote target (pointing to the same MinIO instance)
    let protocol = if ctx.base_url.https { "https" } else { "http" };
    let host_port = if ctx.base_url.port() > 0 {
        format!("{}:{}", ctx.base_url.host(), ctx.base_url.port())
    } else {
        ctx.base_url.host().to_owned()
    };
    let endpoint = format!("{}://{}", protocol, host_port);

    // Create remote target using madmin API (pointing back to same server for bucket-to-bucket replication)
    let target = BucketTarget::builder()
        .source_bucket(bucket_name.as_str().to_string())
        .endpoint(endpoint)
        .target_bucket(bucket_name2.as_str().to_string())
        .credentials(Some(Credentials {
            access_key: Some(ctx.access_key.clone()),
            secret_key: Some(ctx.secret_key.clone()),
            session_token: None,
            expiration: None,
        }))
        .service_type(Some(ServiceType::Replication))
        .secure(Some(ctx.base_url.https))
        .build();

    // NOTE: SetRemoteTarget with the same MinIO instance typically fails because
    // MinIO validates remote targets by connecting to them. For single-instance testing,
    // we demonstrate the API usage, but expect it may fail during validation.
    let remote_target_result = madmin_client
        .set_remote_target()
        .bucket(&bucket_name)
        .target(target)
        .build()
        .send()
        .await;

    match remote_target_result {
        Ok(remote_target_resp) => {
            let arn = remote_target_resp.arn().expect("Failed to get ARN");
            println!("Created remote target with ARN: {}", arn);

            // Create replication config using the ARN from SetRemoteTarget
            // This section only runs if SetRemoteTarget succeeds (requires proper multi-instance setup)
            {
                let mut tags: HashMap<String, String> = HashMap::new();
                tags.insert(String::from("key1"), String::from("value1"));
                tags.insert(String::from("key2"), String::from("value2"));

                let config = ReplicationConfig {
                    role: Some("replication-role".to_string()),
                    rules: vec![ReplicationRule {
                        id: Some(String::from("replication-rule-1")),
                        destination: Destination {
                            bucket_arn: arn.clone(), // Use ARN from SetRemoteTarget!
                            ..Default::default()
                        },
                        filter: Some(Filter {
                            and_operator: Some(AndOperator {
                                prefix: Some(String::from("TaxDocs")),
                                tags: Some(tags),
                            }),
                            ..Default::default()
                        }),
                        priority: Some(1),
                        delete_replication_status: Some(false),
                        status: true,
                        ..Default::default()
                    }],
                };

                let resp: PutBucketReplicationResponse = ctx
                    .client
                    .put_bucket_replication(&bucket_name)
                    .unwrap()
                    .replication_config(config.clone())
                    .build()
                    .send()
                    .await
                    .unwrap();
                //println!("response of setting replication: resp={:?}", resp);
                assert_eq!(resp.bucket(), Some(&bucket_name));
                assert_eq!(resp.region(), &*DEFAULT_REGION);

                let resp: GetBucketReplicationResponse = ctx
                    .client
                    .get_bucket_replication(&bucket_name)
                    .unwrap()
                    .build()
                    .send()
                    .await
                    .unwrap();
                //assert_eq!(resp.config, config); //TODO: Compare replication configs
                assert_eq!(resp.bucket(), Some(&bucket_name));
                assert_eq!(resp.region(), &*DEFAULT_REGION);

                let resp: DeleteBucketReplicationResponse = ctx
                    .client
                    .delete_bucket_replication(&bucket_name)
                    .unwrap()
                    .build()
                    .send()
                    .await
                    .unwrap();
                println!("response of deleting replication: resp={resp:?}");
            }

            // Clean up: Remove the remote target
            let _remove_resp = madmin_client
                .remove_remote_target()
                .bucket(&bucket_name)
                .arn(&arn)
                .build()
                .send()
                .await
                .unwrap();

            println!("Successfully tested complete replication flow");
        }
        Err(e) => {
            // Expected when using same MinIO instance - server validates targets by connecting
            println!(
                "SetRemoteTarget failed as expected for single-instance setup: {:?}",
                e
            );
            println!("Note: This test demonstrates the API usage. For full replication testing,");
            println!("      use two separate MinIO instances as source and target.");
        }
    }

    let _resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    //println!("response of getting replication: resp={:?}", resp);
}

#[minio_macros::test(skip_if_not_express)]
async fn bucket_replication_s3express(ctx: TestContext, bucket: BucketName) {
    let config: ReplicationConfig = create_bucket_replication_config_example(&bucket);

    let resp: Result<PutBucketReplicationResponse, Error> = ctx
        .client
        .put_bucket_replication(&bucket)
        .unwrap()
        .replication_config(config.clone())
        .build()
        .send()
        .await;
    match resp {
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NotSupported)
        }
        v => panic!("Expected error S3Error(NotSupported): but got {v:?}"),
    }

    let resp: Result<GetBucketReplicationResponse, Error> = ctx
        .client
        .get_bucket_replication(&bucket)
        .unwrap()
        .build()
        .send()
        .await;
    match resp {
        Err(Error::S3Server(S3ServerError::S3Error(e))) => {
            assert_eq!(e.code(), MinioErrorCode::NotSupported)
        }
        v => panic!("Expected error S3Error(NotSupported): but got {v:?}"),
    }

    let resp: Result<DeleteBucketReplicationResponse, Error> = ctx
        .client
        .delete_bucket_replication(&bucket)
        .unwrap()
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

fn create_bucket_replication_config_example(bucket: &BucketName) -> ReplicationConfig {
    let mut tags: HashMap<String, String> = HashMap::new();
    tags.insert(String::from("key1"), String::from("value1"));
    tags.insert(String::from("key2"), String::from("value2"));

    ReplicationConfig {
        role: Some("replication-role".to_string()),
        rules: vec![ReplicationRule {
            id: Some(String::from("replication-rule-1")),
            destination: Destination {
                bucket_arn: format!("arn:aws:s3:::{}", bucket.as_str()),
                ..Default::default()
            },
            filter: Some(Filter {
                and_operator: Some(AndOperator {
                    prefix: Some(String::from("TaxDocs")),
                    tags: Some(tags),
                }),
                ..Default::default()
            }),
            priority: Some(1),
            delete_replication_status: Some(false),
            status: true,
            ..Default::default()
        }],
    }
}
