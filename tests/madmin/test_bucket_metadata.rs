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

use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::{ExportBucketMetadataResponse, ImportBucketMetadataResponse};
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio::s3::types::BucketName;
use minio_common::test_context::TestContext;

#[minio_macros::test(skip_if_express)]
async fn test_export_bucket_metadata(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Export metadata from the test bucket
    let resp: ExportBucketMetadataResponse = madmin_client
        .export_bucket_metadata(bucket_name.as_str())
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify we got some data back
    assert!(!resp.data().is_empty(), "Export should return metadata");

    // Metadata is typically in ZIP format containing JSON files
    // Just verify we got bytes back - actual content validation would
    // require ZIP parsing
    println!(
        "Exported {} bytes of metadata for bucket '{}'",
        resp.data().len(),
        bucket_name
    );
}

#[minio_macros::test(skip_if_express)]
async fn test_import_bucket_metadata(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // First export metadata
    let export_resp: ExportBucketMetadataResponse = madmin_client
        .export_bucket_metadata(bucket_name.as_str())
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Then import it back (should be idempotent)
    let import_resp: ImportBucketMetadataResponse = madmin_client
        .import_bucket_metadata(bucket_name.as_str(), export_resp.data().clone())
        .unwrap()
        .build()
        .send()
        .await
        .expect("Import bucket metadata failed");

    // Check the import result
    let result = import_resp.result().expect("Failed to parse result");
    if let Some(buckets) = result.buckets {
        if let Some(status) = buckets.get(bucket_name.as_str()) {
            // Check for any errors
            if let Some(err) = &status.err {
                println!("Bucket import error: {}", err);
            }

            // Log import status for each metadata type
            println!("Import status for bucket '{}':", bucket_name);
            println!("  Object Lock: {}", status.object_lock.is_set);
            println!("  Versioning: {}", status.versioning.is_set);
            println!("  Policy: {}", status.policy.is_set);
            println!("  Tagging: {}", status.tagging.is_set);
            println!("  SSE: {}", status.sse_config.is_set);
            println!("  Lifecycle: {}", status.lifecycle.is_set);
            println!("  Notification: {}", status.notification.is_set);
            println!("  Quota: {}", status.quota.is_set);
            println!("  CORS: {}", status.cors.is_set);
            println!("  QoS: {}", status.qos.is_set);
        } else {
            println!("No status returned for bucket '{}'", bucket_name);
        }
    } else {
        println!("Import completed with no bucket status");
    }
}

#[minio_macros::test(skip_if_express)]
async fn test_export_import_roundtrip(ctx: TestContext, bucket_name: BucketName) {
    let provider = StaticProvider::new(&ctx.access_key, &ctx.secret_key, None);
    let madmin_client = MadminClient::new(ctx.base_url.clone(), Some(provider));

    // Export metadata
    let export_resp: ExportBucketMetadataResponse = madmin_client
        .export_bucket_metadata(bucket_name.as_str())
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    let exported_size = export_resp.data().len();
    assert!(exported_size > 0, "Exported metadata should not be empty");

    // Import the exported metadata
    let import_resp: ImportBucketMetadataResponse = madmin_client
        .import_bucket_metadata(bucket_name.as_str(), export_resp.data().clone())
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();

    // Verify import completed without bucket-level errors
    let result = import_resp.result().expect("Failed to parse result");
    if let Some(buckets) = result.buckets
        && let Some(status) = buckets.get(bucket_name.as_str())
    {
        // A bucket-level error would indicate a problem
        if let Some(err) = &status.err {
            panic!("Unexpected bucket-level error: {}", err);
        }
    }

    println!(
        "Successfully completed export/import roundtrip for bucket '{}' ({} bytes)",
        bucket_name, exported_size
    );
}
