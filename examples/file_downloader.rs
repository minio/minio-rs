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

use crate::common::{create_bucket_if_not_exists, create_client};
use minio::s3::MinioClient;
use minio::s3::builders::ObjectContent;
use minio::s3::types::{ObjectKey, S3Api};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher
    let client: MinioClient = create_client()?;

    let bucket: &str = "file-download-rust-bucket";
    let object: &str = "cat.png";

    // File we are going to upload to the bucket
    let filename: &Path = Path::new("./examples/cat.png");
    let download_path: &str = &format!("/tmp/downloads/{object}");

    create_bucket_if_not_exists(bucket, &client).await?;

    if filename.exists() {
        log::info!("File '{}' exists.", &filename.to_str().unwrap());
    } else {
        log::error!("File '{}' does not exist.", &filename.to_str().unwrap());
        return Ok(());
    }

    let content = ObjectContent::from(filename);
    client
        .put_object_content(bucket, ObjectKey::new(object)?, content)?
        .build()
        .send()
        .await?;

    log::info!(
        "file '{}' is successfully uploaded as object '{object}' to bucket '{bucket}'.",
        filename.display()
    );

    let get_object = client
        .get_object(bucket, ObjectKey::new(object)?)?
        .build()
        .send()
        .await?;

    get_object
        .content()?
        .to_file(Path::new(download_path))
        .await?;

    log::info!("Object '{object}' is successfully downloaded to file '{download_path}'.");

    Ok(())
}
