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

//! Verifies storage-class on writes and checksum-mode on GET/HEAD against a
//! live server (defaults to play.min.io). Run with:
//!
//! ```not_rust
//! cargo run --example storage_class_checksum
//! ```

use futures_util::StreamExt;
use minio::s3::builders::{CopySource, ObjectContent};
use minio::s3::response_traits::HasChecksumHeaders;
use minio::s3::types::{BucketName, ObjectKey, S3Api, ToStream};
use minio::s3::utils::ChecksumAlgorithm;
use minio::s3::{MinioClient, MinioClientBuilder, creds::StaticProvider};
use uuid::Uuid;

const STORAGE_CLASS: &str = "REDUCED_REDUNDANCY";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint =
        std::env::var("SERVER_ENDPOINT").unwrap_or_else(|_| "https://play.min.io".to_string());
    let access_key =
        std::env::var("ACCESS_KEY").unwrap_or_else(|_| "Q3AM3UQ867SPQQA43P2F".to_string());
    let secret_key = std::env::var("SECRET_KEY")
        .unwrap_or_else(|_| "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG".to_string());

    let client: MinioClient = MinioClientBuilder::new(endpoint.parse()?)
        .provider(Some(StaticProvider::new(&access_key, &secret_key, None)))
        .build()?;

    let bucket = format!("test-sc-cs-{}", Uuid::new_v4());
    client.create_bucket(&bucket)?.build().send().await?;
    println!("created bucket {bucket}");

    let mut failures = 0;

    // --- storage_class via put_object_content (single-shot) ---
    let object = "single.txt";
    client
        .put_object_content(&bucket, object, ObjectContent::from("hello storage class"))?
        .storage_class(Some(STORAGE_CLASS.to_string()))
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await?;

    let listed = find_storage_class(&client, &bucket, object).await?;
    if listed.as_deref() == Some(STORAGE_CLASS) {
        println!("PASS put_object_content storage_class -> {listed:?}");
    } else {
        println!("FAIL put_object_content storage_class -> {listed:?} (expected {STORAGE_CLASS})");
        failures += 1;
    }

    // --- storage_class via the single-shot PutObject builder ---
    let object_put = "put.txt";
    let data = minio::s3::segmented_bytes::SegmentedBytes::from("hello put object".to_string());
    client
        .put_object(&bucket, object_put, data)?
        .build()
        .storage_class(STORAGE_CLASS)
        .send()
        .await?;
    let listed_put = find_storage_class(&client, &bucket, object_put).await?;
    if listed_put.as_deref() == Some(STORAGE_CLASS) {
        println!("PASS put_object storage_class -> {listed_put:?}");
    } else {
        println!("FAIL put_object storage_class -> {listed_put:?} (expected {STORAGE_CLASS})");
        failures += 1;
    }

    // --- storage_class through the multipart path (CreateMultipartUpload) ---
    // 6 MiB with a 5 MiB part size forces a 2-part multipart upload, exercising
    // the CreateMultipartUpload branch (storage class set on create, not parts).
    let object_mpu = "multipart.bin";
    client
        .put_object_content(
            &bucket,
            object_mpu,
            ObjectContent::from(vec![b'x'; 6 * 1024 * 1024]),
        )?
        .part_size(Some(5 * 1024 * 1024_u64))
        .storage_class(Some(STORAGE_CLASS.to_string()))
        .build()
        .send()
        .await?;
    let listed_mpu = find_storage_class(&client, &bucket, object_mpu).await?;
    if listed_mpu.as_deref() == Some(STORAGE_CLASS) {
        println!("PASS multipart storage_class -> {listed_mpu:?}");
    } else {
        println!("FAIL multipart storage_class -> {listed_mpu:?} (expected {STORAGE_CLASS})");
        failures += 1;
    }

    // --- storage_class on CopyObject ---
    let object_copy = "copy.txt";
    client
        .copy_object(&bucket, object_copy)?
        .source(
            CopySource::builder()
                .bucket(BucketName::new(&bucket)?)
                .object(ObjectKey::new(object)?)
                .build(),
        )
        .storage_class(Some(STORAGE_CLASS.to_string()))
        .build()
        .send()
        .await?;
    let listed_copy = find_storage_class(&client, &bucket, object_copy).await?;
    if listed_copy.as_deref() == Some(STORAGE_CLASS) {
        println!("PASS copy_object storage_class -> {listed_copy:?}");
    } else {
        println!("FAIL copy_object storage_class -> {listed_copy:?} (expected {STORAGE_CLASS})");
        failures += 1;
    }

    // --- checksum_mode on stat_object (HEAD) ---
    let stat = client
        .stat_object(&bucket, object)?
        .enable_checksum(true)
        .build()
        .send()
        .await?;
    let stat_checksum = stat.get_checksum(ChecksumAlgorithm::CRC32C);
    if stat_checksum.is_some() {
        println!("PASS stat_object checksum_mode -> crc32c {stat_checksum:?}");
    } else {
        println!("FAIL stat_object checksum_mode -> no checksum returned");
        failures += 1;
    }

    // --- checksum_mode on get_object (GET) ---
    let get = client
        .get_object(&bucket, object)?
        .enable_checksum(true)
        .build()
        .send()
        .await?;
    let get_checksum = get.get_checksum(ChecksumAlgorithm::CRC32C);
    if get_checksum.is_some() {
        println!("PASS get_object checksum_mode -> crc32c {get_checksum:?}");
    } else {
        println!("FAIL get_object checksum_mode -> no checksum returned");
        failures += 1;
    }

    // --- cleanup ---
    for obj in [object, object_put, object_mpu, object_copy] {
        let _ = client.delete_object(&bucket, obj)?.build().send().await;
    }
    client.delete_bucket(&bucket)?.build().send().await?;
    println!("removed bucket {bucket}");

    if failures == 0 {
        println!("\nALL CHECKS PASSED");
        Ok(())
    } else {
        Err(format!("{failures} check(s) failed").into())
    }
}

async fn find_storage_class(
    client: &MinioClient,
    bucket: &str,
    object: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = client
        .list_objects(bucket)
        .unwrap()
        .recursive(true)
        .build()
        .to_stream()
        .await;
    while let Some(items) = stream.next().await {
        for item in items?.contents {
            if item.name == object {
                return Ok(item.storage_class);
            }
        }
    }
    Ok(None)
}
