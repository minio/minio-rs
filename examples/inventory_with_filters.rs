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

//! Inventory job with filters example.
//!
//! This example demonstrates creating an inventory job with various filters.

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::inventory::{
    DestinationSpec, FilterSpec, JobDefinition, LastModifiedFilter, ModeSpec, NameFilter, OnOrOff,
    OutputFormat, Schedule, SizeFilter, VersionsSpec,
};
use minio::s3::types::S3Api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None)?;

    let source_bucket = "large-bucket";
    let dest_bucket = "filtered-reports";
    let job_id = "filtered-pdf-inventory";

    println!("Creating filtered inventory job...");

    let filters = FilterSpec {
        prefix: Some(vec!["documents/".to_string(), "reports/".to_string()]),
        last_modified: Some(LastModifiedFilter {
            older_than: None,
            newer_than: Some("30d".to_string()),
            before: None,
            after: None,
        }),
        size: Some(SizeFilter {
            less_than: Some("100MiB".to_string()),
            greater_than: Some("1KiB".to_string()),
            equal_to: None,
        }),
        name: Some(vec![NameFilter {
            match_pattern: Some("*.pdf".to_string()),
            contains: None,
            regex: None,
        }]),
        versions_count: None,
        tags: None,
        user_metadata: None,
    };

    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.to_string(),
            prefix: Some("filtered/".to_string()),
            format: OutputFormat::Parquet,
            compression: OnOrOff::On,
            max_file_size_hint: Some(256 * 1024 * 1024), // 256MB
        },
        schedule: Schedule::Weekly,
        mode: ModeSpec::Strict,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: Some(filters),
    };

    client
        .put_inventory_config(source_bucket, job_id, job)
        .build()
        .send()
        .await?;

    println!("Filtered inventory job '{job_id}' created successfully!");
    println!("This job will find:");
    println!("  - PDF files");
    println!("  - In 'documents/' or 'reports/' directories");
    println!("  - Modified in the last 30 days");
    println!("  - Between 1 KiB and 100 MiB in size");

    Ok(())
}
