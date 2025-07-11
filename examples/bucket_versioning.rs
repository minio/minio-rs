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
use minio::s3::builders::VersioningStatus;
use minio::s3::response::{GetBucketVersioningResponse, PutBucketVersioningResponse};
use minio::s3::types::S3Api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: MinioClient = create_client_on_play()?;

    let bucket_name: &str = "versioning-rust-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    let resp: GetBucketVersioningResponse = client
        .get_bucket_versioning(bucket_name)
        .build()
        .send()
        .await?;
    log::info!(
        "versioning before: status={:?}, mfa_delete={:?}",
        resp.status(),
        resp.mfa_delete()
    );

    let _resp: PutBucketVersioningResponse = client
        .put_bucket_versioning(bucket_name)
        .versioning_status(VersioningStatus::Enabled)
        .build()
        .send()
        .await?;

    let resp: GetBucketVersioningResponse = client
        .get_bucket_versioning(bucket_name)
        .build()
        .send()
        .await?;

    log::info!(
        "versioning after setting to Enabled: status={:?}, mfa_delete={:?}",
        resp.status(),
        resp.mfa_delete()
    );

    let _resp: PutBucketVersioningResponse = client
        .put_bucket_versioning(bucket_name)
        .versioning_status(VersioningStatus::Suspended)
        .build()
        .send()
        .await?;

    let resp: GetBucketVersioningResponse = client
        .get_bucket_versioning(bucket_name)
        .build()
        .send()
        .await?;

    log::info!(
        "versioning after setting to Suspended: status={:?}, mfa_delete={:?}",
        resp.status(),
        resp.mfa_delete()
    );

    let _resp: PutBucketVersioningResponse = client
        .put_bucket_versioning(bucket_name)
        //.versioning_status(VersioningStatus::Suspended)
        .build()
        .send()
        .await?;

    let resp: GetBucketVersioningResponse = client
        .get_bucket_versioning(bucket_name)
        .build()
        .send()
        .await?;

    log::info!(
        "versioning after setting to None: status={:?}, mfa_delete={:?}",
        resp.status(),
        resp.mfa_delete()
    );

    Ok(())
}
