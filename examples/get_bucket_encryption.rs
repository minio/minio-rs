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

use crate::common::{create_bucket_if_not_exists, create_client_on_play};
use minio::s3::builders::GetBucketEncryption;
use minio::s3::Client;

mod common;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: Client = create_client_on_play()?;

    let bucket_name: &str = "encryption-rust-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    let be: GetBucketEncryption = client.get_bucket_encryption(bucket_name);

    log::info!("{:?}", be);

    Ok(())
}
