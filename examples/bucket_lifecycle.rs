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

mod common;

use crate::common::{create_bucket_if_not_exists, create_client_on_play};
use minio::s3::MinioClient;
use minio::s3::lifecycle_config::{LifecycleConfig, LifecycleRule};
use minio::s3::response::{
    DeleteBucketLifecycleResponse, GetBucketLifecycleResponse, PutBucketLifecycleResponse,
};
use minio::s3::types::{Filter, S3Api};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: MinioClient = create_client_on_play()?;

    let bucket_name: &str = "lifecycle-rust-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    if false {
        // TODO
        let resp: GetBucketLifecycleResponse = client
            .get_bucket_lifecycle(bucket_name)
            .build()
            .send()
            .await?;
        log::info!("life cycle settings before setting: resp={resp:?}");
    }

    let rules: Vec<LifecycleRule> = vec![LifecycleRule {
        id: String::from("rule1"),
        expiration_days: Some(365),
        filter: Filter {
            prefix: Some(String::from("logs/")),
            ..Default::default()
        },
        status: true,
        ..Default::default()
    }];

    let resp: PutBucketLifecycleResponse = client
        .put_bucket_lifecycle(bucket_name)
        .life_cycle_config(LifecycleConfig { rules })
        .build()
        .send()
        .await?;
    log::info!("response of setting life cycle config: resp={resp:?}");

    if false {
        // TODO
        let resp: GetBucketLifecycleResponse = client
            .get_bucket_lifecycle(bucket_name)
            .build()
            .send()
            .await?;
        log::info!("life cycle settings after setting: resp={resp:?}");
    }

    if false {
        // TODO
        let resp: DeleteBucketLifecycleResponse = client
            .delete_bucket_lifecycle(bucket_name)
            .build()
            .send()
            .await?;
        log::info!("response of deleting lifecycle config: resp={resp:?}");
    }
    Ok(())
}
