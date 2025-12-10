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

//! Tests for client configuration options like skip_region_lookup.

use bytes::Bytes;
use minio::s3::MinioClient;
use minio::s3::client::MinioClientBuilder;
use minio::s3::creds::StaticProvider;
use minio::s3::response::{GetObjectResponse, PutObjectContentResponse};
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{S3Api, ToStream};
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

/// Helper to create a client with skip_region_lookup enabled.
fn create_client_with_skip_region_lookup(ctx: &TestContext) -> MinioClient {
    let mut builder = MinioClientBuilder::new(ctx.base_url.clone())
        .provider(Some(StaticProvider::new(
            &ctx.access_key,
            &ctx.secret_key,
            None,
        )))
        .skip_region_lookup(true);

    if let Some(ignore_cert) = ctx.ignore_cert_check {
        builder = builder.ignore_cert_check(Some(ignore_cert));
    }

    if let Some(ref ssl_cert_file) = ctx.ssl_cert_file {
        builder = builder.ssl_cert_file(Some(ssl_cert_file));
    }

    builder.build().unwrap()
}

/// Test that skip_region_lookup allows basic put/get operations.
/// This verifies operations work correctly when region lookup is skipped.
#[minio_macros::test]
async fn skip_region_lookup_put_get_object(ctx: TestContext, bucket_name: String) {
    let client = create_client_with_skip_region_lookup(&ctx);
    let object_name = rand_object_name();
    let data: Bytes = Bytes::from("test data with skip_region_lookup");

    // Put object using client with skip_region_lookup
    let put_resp: PutObjectContentResponse = client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(put_resp.bucket(), bucket_name);
    assert_eq!(put_resp.object(), object_name);

    // Get object using the same client
    let get_resp: GetObjectResponse = client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.bucket(), bucket_name);
    assert_eq!(get_resp.object(), object_name);

    let got = get_resp.into_bytes().await.unwrap();
    assert_eq!(got, data);
}

/// Test that skip_region_lookup works for bucket operations.
#[minio_macros::test]
async fn skip_region_lookup_bucket_exists(ctx: TestContext, bucket_name: String) {
    let client = create_client_with_skip_region_lookup(&ctx);

    // Check bucket exists using client with skip_region_lookup
    let exists = client
        .bucket_exists(&bucket_name)
        .build()
        .send()
        .await
        .unwrap()
        .exists();

    assert!(exists, "Bucket should exist");
}

/// Test that skip_region_lookup works for list operations.
#[minio_macros::test]
async fn skip_region_lookup_list_objects(ctx: TestContext, bucket_name: String) {
    let client = create_client_with_skip_region_lookup(&ctx);

    // List objects using client with skip_region_lookup
    // Just verify the operation completes without error
    let mut stream = client.list_objects(&bucket_name).build().to_stream().await;

    use futures_util::StreamExt;
    // Consume the stream - may be empty, but should not error
    let mut count = 0;
    while let Some(result) = stream.next().await {
        // Just verify we can read items without error
        let _item = result.unwrap();
        count += 1;
        // Don't iterate forever
        if count > 100 {
            break;
        }
    }

    // Test passes if we get here without error
}

/// Test that multiple operations work in sequence with skip_region_lookup.
/// This verifies that the default region is consistently used.
#[minio_macros::test]
async fn skip_region_lookup_multiple_operations(ctx: TestContext, bucket_name: String) {
    let client = create_client_with_skip_region_lookup(&ctx);

    // Perform multiple operations to ensure consistent behavior
    for i in 0..3 {
        let object_name = format!("test-object-{}", i);
        let data: Bytes = Bytes::from(format!("data for object {}", i));

        // Put
        client
            .put_object_content(&bucket_name, &object_name, data.clone())
            .build()
            .send()
            .await
            .unwrap();

        // Get
        let resp: GetObjectResponse = client
            .get_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();

        let got = resp.into_bytes().await.unwrap();
        assert_eq!(got, data);

        // Delete
        client
            .delete_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();
    }
}

/// Test that skip_region_lookup does not affect stat_object operations.
#[minio_macros::test]
async fn skip_region_lookup_stat_object(ctx: TestContext, bucket_name: String) {
    let client = create_client_with_skip_region_lookup(&ctx);
    let object_name = rand_object_name();
    let data: Bytes = Bytes::from("test data for stat");

    // Put object
    client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Stat object using client with skip_region_lookup
    let stat_resp = client
        .stat_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(stat_resp.bucket(), bucket_name);
    assert_eq!(stat_resp.object(), object_name);
    assert_eq!(stat_resp.size().unwrap(), data.len() as u64);
}
