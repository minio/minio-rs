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

//! Example demonstrating how to use RequestHooks for debug logging.
//!
//! This example shows:
//! - Creating a custom debug logging hook
//! - Attaching the hook to the MinIO client
//! - Automatic logging of all S3 API requests with headers and response status
//! - Using both `before_signing_mut` and `after_execute` hooks
//!
//! Run with default values (test-bucket / test-object.txt, verbose mode enabled):
//! ```
//! cargo run --example debug_logging_hook
//! ```
//!
//! Run with custom bucket and object:
//! ```
//! cargo run --example debug_logging_hook -- mybucket myobject
//! ```
//!
//! Disable verbose output:
//! ```
//! cargo run --example debug_logging_hook -- --no-verbose
//! ```

use clap::{ArgAction, Parser};
use futures_util::StreamExt;
use minio::s3::builders::ObjectContent;
use minio::s3::client::hooks::{Extensions, RequestHooks};
use minio::s3::client::{Method, Response};
use minio::s3::creds::StaticProvider;
use minio::s3::error::Error;
use minio::s3::http::Url;
use minio::s3::multimap_ext::Multimap;
use minio::s3::response::BucketExistsResponse;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::{BucketName, ObjectKey, S3Api, ToStream};
use minio::s3::{MinioClient, MinioClientBuilder};
use std::sync::Arc;

/// Debug logging hook that prints detailed information about each S3 request.
#[derive(Debug)]
struct DebugLoggingHook {
    /// Enable verbose output including all headers
    verbose: bool,
}

impl DebugLoggingHook {
    fn new(verbose: bool) -> Self {
        Self { verbose }
    }
}

#[async_trait::async_trait]
impl RequestHooks for DebugLoggingHook {
    fn name(&self) -> &'static str {
        "debug-logger"
    }

    async fn before_signing_mut(
        &self,
        method: &Method,
        url: &mut Url,
        _region: &str,
        _headers: &mut Multimap,
        _query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        _body: Option<&SegmentedBytes>,
        _extensions: &mut Extensions,
    ) -> Result<(), Error> {
        if self.verbose {
            let bucket_obj = match (bucket_name, object_name) {
                (Some(b), Some(o)) => format!("{b}/{o}"),
                (Some(b), None) => b.to_string(),
                _ => url.to_string(),
            };
            println!("→ Preparing {method} request for {bucket_obj}");
        }
        Ok(())
    }

    async fn after_execute(
        &self,
        method: &Method,
        url: &Url,
        _region: &str,
        headers: &Multimap,
        _query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        resp: &Result<Response, reqwest::Error>,
        _extensions: &mut Extensions,
    ) {
        // Format the basic request info
        let bucket_obj = match (bucket_name, object_name) {
            (Some(b), Some(o)) => format!("{b}/{o}"),
            (Some(b), None) => b.to_string(),
            _ => url.to_string(),
        };

        // Format response status
        let status = match resp {
            Ok(response) => format!("✓ {}", response.status()),
            Err(err) => format!("✗ Error: {err}"),
        };

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("S3 Request: {method} {bucket_obj}");
        println!("URL: {url}");
        println!("Status: {status}");

        if self.verbose {
            // Print headers alphabetically
            let mut header_strings: Vec<String> = headers
                .iter_all()
                .map(|(k, v)| format!("{}: {}", k, v.join(",")))
                .collect();
            header_strings.sort();

            println!("\nRequest Headers:");
            for header in header_strings {
                println!("  {header}");
            }
        }

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    }
}

/// Example demonstrating debug logging with hooks
#[derive(Parser)]
struct Cli {
    /// Bucket to use for the example
    #[arg(default_value = "test-bucket")]
    bucket: String,
    /// Object to upload
    #[arg(default_value = "test-object.txt")]
    object: String,
    /// Disable verbose output (verbose is enabled by default, use --no-verbose to disable)
    #[arg(long = "no-verbose", action = ArgAction::SetFalse, default_value_t = true)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    let args = Cli::parse();

    println!("\n🔧 MinIO Debug Logging Hook Example\n");
    println!("This example demonstrates how hooks can be used for debugging S3 requests.");
    println!(
        "We'll perform a few operations on bucket '{}' with debug logging enabled.\n",
        args.bucket
    );

    // Create the debug logging hook
    let debug_hook = Arc::new(DebugLoggingHook::new(args.verbose));

    // Create MinIO client with the debug logging hook attached
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);

    let client: MinioClient = MinioClientBuilder::new("http://localhost:9000".parse()?)
        .provider(Some(static_provider))
        .hook(debug_hook) // Attach the debug logging hook
        .build()?;

    println!("✓ Created MinIO client with debug logging hook\n");

    // Operation 1: Check if bucket exists
    println!("📋 Checking if bucket exists...");
    let resp: BucketExistsResponse = client
        .bucket_exists(BucketName::new(&args.bucket)?)
        .build()
        .send()
        .await?;

    // Operation 2: Create bucket if it doesn't exist
    if !resp.exists() {
        println!("\n📋 Creating bucket...");
        client
            .create_bucket(BucketName::new(&args.bucket)?)
            .build()
            .send()
            .await?;
    } else {
        println!("\n✓ Bucket already exists");
    }

    // Operation 3: Upload a small object
    println!("\n📋 Uploading object...");
    let content = b"Hello from MinIO Rust SDK with debug logging!";
    let object_content: ObjectContent = content.to_vec().into();
    client
        .put_object_content(
            BucketName::new(&args.bucket)?,
            ObjectKey::new(&args.object)?,
            object_content,
        )
        .build()
        .send()
        .await?;

    // Operation 4: List objects in the bucket
    println!("\n📋 Listing objects in bucket...");
    let mut list_stream = client
        .list_objects(BucketName::new(&args.bucket)?)
        .recursive(false)
        .build()
        .to_stream()
        .await;

    let mut total_objects = 0;
    while let Some(result) = list_stream.next().await {
        match result {
            Ok(resp) => {
                total_objects += resp.contents.len();
            }
            Err(e) => {
                eprintln!("Error listing objects: {e}");
            }
        }
    }
    println!("\n✓ Found {total_objects} objects in bucket");

    println!("\n🎉 All operations completed successfully with debug logging enabled!\n");
    println!("💡 Tip: Run with --no-verbose to disable detailed output\n");

    Ok(())
}
