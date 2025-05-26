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
use minio::s3::types::S3Api;
use minio::s3::{Client, builders::ObjectContent, client::ClientBuilder, creds::StaticProvider};
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

    let static_provider = StaticProvider::new(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
        None,
    );

    let client: Client = ClientBuilder::new("https://play.min.io".parse()?)
        .provider(Some(static_provider))
        .build()?;

    let resp: BucketExistsResponse = client.bucket_exists(&args.bucket).send().await.unwrap();

    if !resp.exists() {
        client.create_bucket(&args.bucket).send().await.unwrap();
    }

    let content = ObjectContent::from(args.file.as_path());
    // Put an object
    client
        .put_object_content(&args.bucket, &args.object, content)
        .send()
        .await?;

    info!(
        "Uploaded file at {:?} to {}/{}",
        args.file, args.bucket, args.object
    );

    Ok(())
}
