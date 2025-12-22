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

use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::response::BucketExistsResponse;
use minio::s3::types::{BucketName, S3Api};
use minio::s3::{MinioClient, MinioClientBuilder};

#[allow(dead_code)]
pub fn create_client() -> Result<MinioClient, Box<dyn std::error::Error + Send + Sync>> {
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    log::info!("Trying to connect to MinIO at: `{base_url:?}`");

    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);

    let client = MinioClientBuilder::new(base_url.clone())
        .provider(Some(static_provider))
        .build()?;
    Ok(client)
}

pub async fn create_bucket_if_not_exists(
    bucket_name: &str,
    client: &MinioClient,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check 'bucket_name' bucket exist or not.
    let bucket = BucketName::try_from(bucket_name)?;
    let resp: BucketExistsResponse = client.bucket_exists(bucket.clone()).build().send().await?;

    // Make 'bucket_name' bucket if not exist.
    if !resp.exists() {
        client.create_bucket(bucket).build().send().await?;
    };
    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // dummy code just to prevent an error because files in examples need to have a main
    Ok(())
}
