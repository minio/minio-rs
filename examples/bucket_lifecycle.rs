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
use minio::s3::Client;
use minio::s3::response::{
    DeleteBucketLifecycleResponse, GetBucketLifecycleResponse, SetBucketLifecycleResponse,
};
use minio::s3::types::{Filter, LifecycleConfig, LifecycleRule, S3Api};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: Arc<Client> = create_client_on_play()?;

    let bucket_name: &str = "lifecycle-rust-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    if false {
        // TODO
        let resp: GetBucketLifecycleResponse =
            client.get_bucket_lifecycle(bucket_name).send().await?;
        log::info!("life cycle settings before setting: resp={:?}", resp);
    }

    let rules: Vec<LifecycleRule> = vec![LifecycleRule {
        abort_incomplete_multipart_upload_days_after_initiation: None,
        expiration_date: None,
        expiration_days: Some(365),
        expiration_expired_object_delete_marker: None,
        filter: Filter {
            and_operator: None,
            prefix: Some(String::from("logs/")),
            tag: None,
        },
        id: String::from("rule1"),
        noncurrent_version_expiration_noncurrent_days: None,
        noncurrent_version_transition_noncurrent_days: None,
        noncurrent_version_transition_storage_class: None,
        status: true,
        transition_date: None,
        transition_days: None,
        transition_storage_class: None,
    }];

    let resp: SetBucketLifecycleResponse = client
        .set_bucket_lifecycle(bucket_name)
        .life_cycle_config(LifecycleConfig { rules })
        .send()
        .await?;
    log::info!("response of setting life cycle config: resp={:?}", resp);

    if false {
        // TODO
        let resp: GetBucketLifecycleResponse =
            client.get_bucket_lifecycle(bucket_name).send().await?;
        log::info!("life cycle settings after setting: resp={:?}", resp);
    }

    if false {
        // TODO
        let resp: DeleteBucketLifecycleResponse =
            client.delete_bucket_lifecycle(bucket_name).send().await?;
        log::info!("response of deleting lifecycle config: resp={:?}", resp);
    }
    Ok(())
}
