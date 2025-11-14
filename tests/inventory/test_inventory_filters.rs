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

use minio::s3::inventory::{
    DestinationSpec, Field, FilterSpec, JobDefinition, LastModifiedFilter, ModeSpec, NameFilter,
    OnOrOff, OutputFormat, Schedule, SizeFilter, VersionsSpec,
};
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn inventory_with_filters(ctx: TestContext, bucket_name: String) {
    let job_id = "test-filters-job";
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Create comprehensive filter specification
    let filters = FilterSpec {
        prefix: Some(vec![
            "documents/".to_string(),
            "reports/".to_string(),
            "archives/".to_string(),
        ]),
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

    // Create inventory job with filters and additional fields
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: job_id.to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
            prefix: Some("filtered-reports/".to_string()),
            format: OutputFormat::Parquet,
            compression: OnOrOff::On,
            max_file_size_hint: Some(256 * 1024 * 1024), // 256MB
        },
        schedule: Schedule::Weekly,
        mode: ModeSpec::Strict,
        versions: VersionsSpec::Current,
        include_fields: vec![
            Field::ETag,
            Field::StorageClass,
            Field::Tags,
            Field::UserMetadata,
        ],
        filters: Some(filters),
    };

    // Put inventory config
    ctx.client
        .put_inventory_config(&bucket_name, job_id, job)
        .build()
        .send()
        .await
        .unwrap();

    // Get and verify the configuration
    let config = ctx
        .client
        .get_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(config.id(), job_id);

    let yaml = config.yaml_definition();
    // Verify filters are in YAML
    assert!(yaml.contains("filters:"), "Should contain filters section");
    assert!(yaml.contains("prefix:"), "Should contain prefix filter");
    assert!(
        yaml.contains("documents/"),
        "Should contain documents prefix"
    );
    assert!(
        yaml.contains("lastModified:"),
        "Should contain lastModified filter"
    );
    assert!(yaml.contains("newerThan:"), "Should contain newerThan");
    assert!(yaml.contains("30d"), "Should contain 30d value");
    assert!(yaml.contains("size:"), "Should contain size filter");
    assert!(yaml.contains("100MiB"), "Should contain size limit");
    assert!(yaml.contains("name:"), "Should contain name filter");
    assert!(yaml.contains("*.pdf"), "Should contain PDF pattern");

    // Verify output format and schedule
    assert!(
        yaml.contains("format: parquet"),
        "Should have Parquet format"
    );
    assert!(
        yaml.contains("schedule: weekly"),
        "Should have weekly schedule"
    );
    assert!(yaml.contains("mode: strict"), "Should have strict mode");

    // Verify additional fields
    assert!(
        yaml.contains("includeFields:"),
        "Should contain includeFields section"
    );
    assert!(yaml.contains("ETag"), "Should include ETag field");
    assert!(
        yaml.contains("StorageClass"),
        "Should include StorageClass field"
    );

    // Cleanup
    ctx.client
        .delete_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .ok();

    ctx.client
        .delete_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();
}

#[minio_macros::test(no_cleanup)]
async fn inventory_different_formats(ctx: TestContext, bucket_name: String) {
    let dest_bucket = format!("{bucket_name}-dest");

    // Create destination bucket (ignore if already exists)
    ctx.client
        .create_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();

    // Test CSV format
    let csv_job = JobDefinition {
        api_version: "v1".to_string(),
        id: "test-csv".to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
            prefix: Some("csv/".to_string()),
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Once,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    ctx.client
        .put_inventory_config(&bucket_name, "test-csv", csv_job)
        .build()
        .send()
        .await
        .unwrap();

    // Test JSON format
    let json_job = JobDefinition {
        api_version: "v1".to_string(),
        id: "test-json".to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
            prefix: Some("json/".to_string()),
            format: OutputFormat::JSON,
            compression: OnOrOff::Off,
            max_file_size_hint: None,
        },
        schedule: Schedule::Daily,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::All,
        include_fields: vec![],
        filters: None,
    };

    ctx.client
        .put_inventory_config(&bucket_name, "test-json", json_job)
        .build()
        .send()
        .await
        .unwrap();

    // Test Parquet format
    let parquet_job = JobDefinition {
        api_version: "v1".to_string(),
        id: "test-parquet".to_string(),
        destination: DestinationSpec {
            bucket: dest_bucket.clone(),
            prefix: Some("parquet/".to_string()),
            format: OutputFormat::Parquet,
            compression: OnOrOff::On,
            max_file_size_hint: Some(512 * 1024 * 1024),
        },
        schedule: Schedule::Monthly,
        mode: ModeSpec::Strict,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    ctx.client
        .put_inventory_config(&bucket_name, "test-parquet", parquet_job)
        .build()
        .send()
        .await
        .unwrap();

    // List and verify all three jobs exist
    // Retry a few times to handle eventual consistency
    let mut attempts = 0;
    let max_attempts = 5;
    let job_ids = loop {
        let list = ctx
            .client
            .list_inventory_configs(&bucket_name)
            .build()
            .send()
            .await
            .unwrap();

        let ids: Vec<String> = list.items().iter().map(|item| item.id.clone()).collect();

        // Check if all three jobs are present
        if ids.iter().any(|id| id == "test-csv")
            && ids.iter().any(|id| id == "test-json")
            && ids.iter().any(|id| id == "test-parquet")
        {
            break ids;
        }

        attempts += 1;
        if attempts >= max_attempts {
            eprintln!(
                "After {} attempts, found {} jobs: {:?}",
                attempts,
                ids.len(),
                ids
            );
            break ids;
        }

        // Wait a bit before retrying
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };

    assert!(
        job_ids.iter().any(|id| id == "test-csv"),
        "CSV job should exist. Found: {job_ids:?}"
    );
    assert!(
        job_ids.iter().any(|id| id == "test-json"),
        "JSON job should exist. Found: {job_ids:?}"
    );
    assert!(
        job_ids.iter().any(|id| id == "test-parquet"),
        "Parquet job should exist. Found: {job_ids:?}"
    );

    // Cleanup
    for job_id in ["test-csv", "test-json", "test-parquet"] {
        ctx.client
            .delete_inventory_config(&bucket_name, job_id)
            .build()
            .send()
            .await
            .ok();
    }

    ctx.client
        .delete_bucket(&dest_bucket)
        .build()
        .send()
        .await
        .ok();
}
