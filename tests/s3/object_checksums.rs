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

use bytes::Bytes;
use minio::s3::builders::{PutObject, UploadPart};
use minio::s3::response::PutObjectResponse;
use minio::s3::response_traits::{HasBucket, HasChecksumHeaders, HasObject};
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use minio::s3::utils::ChecksumAlgorithm;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use std::sync::Arc;

/// Helper function to upload an object with a specific checksum algorithm
async fn upload_with_checksum(
    ctx: &TestContext,
    bucket: &str,
    object: &str,
    data: &[u8],
    algorithm: ChecksumAlgorithm,
) -> PutObjectResponse {
    let inner = UploadPart::builder()
        .client(ctx.client.clone())
        .bucket(bucket.to_string())
        .object(object.to_string())
        .data(Arc::new(SegmentedBytes::from(Bytes::from(data.to_vec()))))
        .checksum_algorithm(algorithm)
        .build();

    PutObject::builder()
        .inner(inner)
        .build()
        .send()
        .await
        .unwrap()
}

/// Test uploading an object with CRC32 checksum
#[minio_macros::test]
async fn upload_with_crc32_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing CRC32 checksum.";

    let resp = upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC32,
    )
    .await;

    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test uploading an object with CRC32C checksum
#[minio_macros::test]
async fn upload_with_crc32c_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing CRC32C checksum.";

    let resp = upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC32C,
    )
    .await;

    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test uploading an object with SHA1 checksum
#[minio_macros::test]
async fn upload_with_sha1_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing SHA1 checksum.";

    let resp = upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::SHA1,
    )
    .await;

    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test uploading an object with SHA256 checksum
#[minio_macros::test]
async fn upload_with_sha256_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing SHA256 checksum.";

    let resp = upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::SHA256,
    )
    .await;

    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test uploading an object with CRC64NVME checksum
#[minio_macros::test]
async fn upload_with_crc64nvme_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing CRC64NVME checksum.";

    let resp = upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC64NVME,
    )
    .await;

    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test round-trip: upload with checksum and download with verification
#[minio_macros::test]
async fn upload_download_with_crc32c_verification(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Round-trip test with CRC32C checksum verification.";

    upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC32C,
    )
    .await;

    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let algorithm = get_resp.detect_checksum_algorithm();
    // Note: Server may or may not return checksums depending on configuration
    // If checksums are available, verify them. If not, just check content matches.
    if let Some(algo) = algorithm {
        assert_eq!(algo, ChecksumAlgorithm::CRC32C);
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(downloaded.as_ref(), data);
    } else {
        // No checksum returned, just verify content
        let downloaded = get_resp.content().unwrap();
        let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), data);
    }

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test round-trip with SHA256
#[minio_macros::test]
async fn upload_download_with_sha256_verification(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Round-trip test with SHA256 checksum verification.";

    upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::SHA256,
    )
    .await;

    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let algorithm = get_resp.detect_checksum_algorithm();
    // Note: Server may or may not return checksums depending on configuration
    // If checksums are available, verify them. If not, just check content matches.
    if let Some(algo) = algorithm {
        assert_eq!(algo, ChecksumAlgorithm::SHA256);
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(downloaded.as_ref(), data);
    } else {
        // No checksum returned, just verify content
        let downloaded = get_resp.content().unwrap();
        let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), data);
    }

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test round-trip with CRC64NVME
#[minio_macros::test]
async fn upload_download_with_crc64nvme_verification(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Round-trip test with CRC64NVME checksum verification.";

    upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC64NVME,
    )
    .await;

    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let algorithm = get_resp.detect_checksum_algorithm();
    // Note: Server may or may not return checksums depending on configuration
    // If checksums are available, verify them. If not, just check content matches.
    if let Some(algo) = algorithm {
        assert_eq!(algo, ChecksumAlgorithm::CRC64NVME);
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(downloaded.as_ref(), data);
    } else {
        // No checksum returned, just verify content
        let downloaded = get_resp.content().unwrap();
        let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), data);
    }

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test that downloading without checksum still works
#[minio_macros::test]
async fn upload_download_without_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Upload without checksum, should work fine.";

    ctx.client
        .put_object(
            &bucket_name,
            &object_name,
            SegmentedBytes::from(String::from_utf8_lossy(data).to_string()),
        )
        .build()
        .send()
        .await
        .unwrap();

    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let algorithm = get_resp.detect_checksum_algorithm();
    assert!(
        algorithm.is_none(),
        "No checksum algorithm should be detected"
    );

    let downloaded = get_resp.content_verified().await.unwrap();
    assert_eq!(downloaded.as_ref(), data);

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test checksum with larger data
#[minio_macros::test]
async fn upload_download_large_data_with_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = vec![0xAB; 1024 * 100]; // 100KB of data

    upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        &data,
        ChecksumAlgorithm::CRC32C,
    )
    .await;

    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content_verified().await.unwrap();
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(downloaded.as_ref(), data.as_slice());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test all checksum algorithms in sequence
#[minio_macros::test]
async fn test_all_checksum_algorithms(ctx: TestContext, bucket_name: String) {
    let algorithms = vec![
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ];

    for algo in algorithms {
        let object_name = format!("checksum-test-{:?}-{}", algo, rand_object_name());
        let data = format!("Testing {:?} checksum algorithm", algo);

        upload_with_checksum(&ctx, &bucket_name, &object_name, data.as_bytes(), algo).await;

        let get_resp = ctx
            .client
            .get_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();

        let detected_algo = get_resp.detect_checksum_algorithm();
        // Note: Server may or may not return checksums depending on configuration
        // If checksums are available, verify them. If not, just check content matches.
        if let Some(detected) = detected_algo {
            assert_eq!(detected, algo, "Algorithm mismatch for {:?}", algo);
            let downloaded = get_resp.content_verified().await.unwrap();
            assert_eq!(downloaded.as_ref(), data.as_bytes());
        } else {
            // No checksum returned, just verify content
            let downloaded = get_resp.content().unwrap();
            let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
            assert_eq!(bytes.as_ref(), data.as_bytes());
        }

        ctx.client
            .delete_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();
    }
}
