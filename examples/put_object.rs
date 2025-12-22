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

use clap::Parser;
use log::info;
use minio::s3::response::BucketExistsResponse;
use minio::s3::types::{BucketName, ObjectKey, S3Api};
use minio::s3::{MinioClient, MinioClientBuilder, builders::ObjectContent, creds::StaticProvider};
use std::path::PathBuf;

/// Upload a file to the given bucket and object path on the MinIO Play server.
#[derive(Parser)]
struct Cli {
    /// Bucket to upload the file to (will be created if it doesn't exist)
    bucket: String,
    /// Object path to upload the file to.
    object: String,
    /// File to upload.
    file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Cli::parse();

    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);

    let client: MinioClient = MinioClientBuilder::new("http://localhost:9000".parse()?)
        .provider(Some(static_provider))
        .build()?;

    let resp: BucketExistsResponse = client
        .bucket_exists(BucketName::new(&args.bucket)?)
        .build()
        .send()
        .await?;

    if !resp.exists() {
        client
            .create_bucket(BucketName::new(&args.bucket)?)
            .build()
            .send()
            .await?;
    }

    let content = ObjectContent::from(args.file.as_path());
    // Put an object
    client
        .put_object_content(
            BucketName::new(&args.bucket)?,
            ObjectKey::new(&args.object)?,
            content,
        )
        .build()
        .send()
        .await?;

    info!(
        "Uploaded file at {:?} to {}/{}",
        args.file, args.bucket, args.object
    );

    Ok(())
}
