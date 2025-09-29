// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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

use crate::common::create_bucket_if_not_exists;
use minio::s3::MinioClientBuilder;
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::response::GetObjectPromptResponse;
use minio::s3::types::S3Api;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher

    //Note: object prompt is not supported on play.min.io, you will need point to AIStor
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    log::info!("Trying to connect to MinIO at: `{base_url:?}`");

    let static_provider = StaticProvider::new("admin", "admin", None);

    let client = MinioClientBuilder::new(base_url.clone())
        .provider(Some(static_provider))
        .ignore_cert_check(Some(true))
        .build()?;

    let bucket_name: &str = "object-prompt-rust-bucket";
    create_bucket_if_not_exists(bucket_name, &client).await?;

    // File we are going to upload to the bucket
    let filename: &Path = Path::new("./examples/cat.png");

    // Name of the object that will be stored in the bucket
    let object_name: &str = "cat.png";

    if filename.exists() {
        log::info!("File '{}' exists.", &filename.to_str().unwrap());
    } else {
        log::error!("File '{}' does not exist.", &filename.to_str().unwrap());
        return Ok(());
    }

    let content = ObjectContent::from(filename);
    client
        .put_object_content(bucket_name, object_name, content)
        .build()
        .send()
        .await?;

    log::info!(
        "File '{}' is successfully uploaded as object '{object_name}' to bucket '{bucket_name}'.",
        filename.display()
    );

    let resp: GetObjectPromptResponse = client
        .get_object_prompt(bucket_name, object_name, "what is it about?")
        //.lambda_arn("arn:minio:s3-object-lambda::_:webhook") // this is the default value
        .build()
        .send()
        .await?;

    log::info!("Object prompt result: '{}'", resp.prompt_response()?);

    Ok(())
}
