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
use minio::s3::response::{GetBucketEncryptionResponse, PutBucketEncryptionResponse};
use minio::s3::types::{S3Api, SseConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: MinioClient = create_client_on_play()?;

    let bucket_name: &str = "encryption-rust-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    let resp: GetBucketEncryptionResponse = client
        .get_bucket_encryption(bucket_name)
        .build()
        .send()
        .await?;
    log::info!("encryption before: config={:?}", resp.config());

    let config = SseConfig::default();
    log::info!("going to set encryption config={config:?}");

    let _resp: PutBucketEncryptionResponse = client
        .put_bucket_encryption(bucket_name)
        .sse_config(config.clone())
        .build()
        .send()
        .await?;

    let resp: GetBucketEncryptionResponse = client
        .get_bucket_encryption(bucket_name)
        .build()
        .send()
        .await?;
    log::info!("encryption after: config={:?}", resp.config());

    Ok(())
}
