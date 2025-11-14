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
use minio::s3::builders::{ComposeSource, CopySource, ObjectContent, PutObject, UploadPart};
use minio::s3::response::{
    AppendObjectResponse, ComposeObjectResponse, CopyObjectResponse, PutObjectContentResponse,
    PutObjectResponse,
};
use minio::s3::response_traits::{HasBucket, HasChecksumHeaders, HasObject, HasObjectSize};
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use minio::s3::utils::ChecksumAlgorithm;
use minio_common::rand_src::RandSrc;
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

// ============================================================================
// CRC32 and SHA1 round-trip tests
// ============================================================================

/// Test round-trip with CRC32
#[minio_macros::test]
async fn upload_download_with_crc32_verification(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Round-trip test with CRC32 checksum verification.";

    upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC32,
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
    if let Some(algo) = algorithm {
        assert_eq!(algo, ChecksumAlgorithm::CRC32);
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(downloaded.as_ref(), data);
    } else {
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

/// Test round-trip with SHA1
#[minio_macros::test]
async fn upload_download_with_sha1_verification(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Round-trip test with SHA1 checksum verification.";

    upload_with_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::SHA1,
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
    if let Some(algo) = algorithm {
        assert_eq!(algo, ChecksumAlgorithm::SHA1);
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(downloaded.as_ref(), data);
    } else {
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

// ============================================================================
// AppendObject checksum tests
// ============================================================================

/// Test AppendObject with CRC32C checksum
#[minio_macros::test(skip_if_not_express)]
async fn append_object_with_crc32c_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let content1 = "Initial content for append test.";
    let content2 = "Appended content with checksum.";

    // Create initial object
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, content1)
        .build()
        .send()
        .await
        .unwrap();

    // Append with checksum
    let data2 = SegmentedBytes::from(content2.to_string());
    let offset = content1.len() as u64;
    let resp: AppendObjectResponse = ctx
        .client
        .append_object(&bucket_name, &object_name, data2, offset)
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), (content1.len() + content2.len()) as u64);

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test AppendObject with all checksum algorithms
#[minio_macros::test(skip_if_not_express)]
async fn append_object_all_checksum_algorithms(ctx: TestContext, bucket_name: String) {
    let algorithms = vec![
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ];

    for algo in algorithms {
        let object_name = format!("append-checksum-{:?}-{}", algo, rand_object_name());
        let content1 = format!("Initial content for {:?}", algo);
        let content2 = format!("Appended with {:?} checksum", algo);

        // Create initial object
        let content1_len = content1.len();
        let _resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &object_name, content1)
            .build()
            .send()
            .await
            .unwrap();

        // Append with checksum
        let data2 = SegmentedBytes::from(content2.clone());
        let offset = content1_len as u64;
        let resp: AppendObjectResponse = ctx
            .client
            .append_object(&bucket_name, &object_name, data2, offset)
            .checksum_algorithm(algo)
            .build()
            .send()
            .await
            .unwrap();

        assert_eq!(resp.bucket(), bucket_name);
        assert_eq!(resp.object(), object_name);
        assert_eq!(
            resp.object_size(),
            (content1_len + content2.len()) as u64,
            "Size mismatch for {:?}",
            algo
        );

        ctx.client
            .delete_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();
    }
}

/// Test AppendObjectContent with checksum
#[minio_macros::test(skip_if_not_express)]
async fn append_object_content_with_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let content1 = "Initial content.";
    let content2 = "Appended content with SHA256.";

    // Create initial object
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, content1)
        .build()
        .send()
        .await
        .unwrap();

    // Append content with checksum
    let resp: AppendObjectResponse = ctx
        .client
        .append_object_content(&bucket_name, &object_name, content2)
        .checksum_algorithm(ChecksumAlgorithm::SHA256)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), (content1.len() + content2.len()) as u64);

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

// ============================================================================
// CopyObject checksum tests
// ============================================================================

/// Test CopyObject with CRC32C checksum
#[minio_macros::test(skip_if_express)]
async fn copy_object_with_crc32c_checksum(ctx: TestContext, bucket_name: String) {
    let src_object = rand_object_name();
    let dst_object = rand_object_name();
    let data = b"Content to copy with checksum verification.";

    // Create source object
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &src_object, Bytes::from_static(data))
        .build()
        .send()
        .await
        .unwrap();

    // Copy with checksum
    let resp: CopyObjectResponse = ctx
        .client
        .copy_object(&bucket_name, &dst_object)
        .source(
            CopySource::builder()
                .bucket(&bucket_name)
                .object(&src_object)
                .build(),
        )
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), dst_object);

    // Verify the copy
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content().unwrap();
    let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), data);

    // Cleanup
    ctx.client
        .delete_object(&bucket_name, &src_object)
        .build()
        .send()
        .await
        .unwrap();
    ctx.client
        .delete_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test CopyObject with all checksum algorithms
#[minio_macros::test(skip_if_express)]
async fn copy_object_all_checksum_algorithms(ctx: TestContext, bucket_name: String) {
    let algorithms = vec![
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ];

    for algo in algorithms {
        let src_object = format!("copy-src-{:?}-{}", algo, rand_object_name());
        let dst_object = format!("copy-dst-{:?}-{}", algo, rand_object_name());
        let data = format!("Content to copy with {:?}", algo);

        // Create source object
        let _resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &src_object, data)
            .build()
            .send()
            .await
            .unwrap();

        // Copy with checksum
        let resp: CopyObjectResponse = ctx
            .client
            .copy_object(&bucket_name, &dst_object)
            .source(
                CopySource::builder()
                    .bucket(&bucket_name)
                    .object(&src_object)
                    .build(),
            )
            .checksum_algorithm(algo)
            .build()
            .send()
            .await
            .unwrap();

        assert_eq!(resp.bucket(), bucket_name, "Bucket mismatch for {:?}", algo);
        assert_eq!(resp.object(), dst_object, "Object mismatch for {:?}", algo);

        // Cleanup
        ctx.client
            .delete_object(&bucket_name, &src_object)
            .build()
            .send()
            .await
            .unwrap();
        ctx.client
            .delete_object(&bucket_name, &dst_object)
            .build()
            .send()
            .await
            .unwrap();
    }
}

// ============================================================================
// ComposeObject checksum tests
// ============================================================================

/// Test ComposeObject with CRC32C checksum
#[minio_macros::test]
async fn compose_object_with_crc32c_checksum(ctx: TestContext, bucket_name: String) {
    let src_object = rand_object_name();
    let dst_object = rand_object_name();
    let data = b"Content to compose with checksum verification.";

    // Create source object
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &src_object, Bytes::from_static(data))
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);

    // Compose with checksum
    let sources = vec![ComposeSource::new(&bucket_name, &src_object).unwrap()];
    let resp: ComposeObjectResponse = ctx
        .client
        .compose_object(&bucket_name, &dst_object, sources)
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), dst_object);

    // Verify the composed object
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content().unwrap();
    let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), data);

    // Cleanup
    ctx.client
        .delete_object(&bucket_name, &src_object)
        .build()
        .send()
        .await
        .unwrap();
    ctx.client
        .delete_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test ComposeObject with all checksum algorithms
#[minio_macros::test]
async fn compose_object_all_checksum_algorithms(ctx: TestContext, bucket_name: String) {
    let algorithms = vec![
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ];

    for algo in algorithms {
        let src_object = format!("compose-src-{:?}-{}", algo, rand_object_name());
        let dst_object = format!("compose-dst-{:?}-{}", algo, rand_object_name());
        let data = format!("Content to compose with {:?}", algo);

        // Create source object
        let _resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &src_object, data)
            .build()
            .send()
            .await
            .unwrap();

        // Compose with checksum
        let sources = vec![ComposeSource::new(&bucket_name, &src_object).unwrap()];
        let resp: ComposeObjectResponse = ctx
            .client
            .compose_object(&bucket_name, &dst_object, sources)
            .checksum_algorithm(algo)
            .build()
            .send()
            .await
            .unwrap();

        assert_eq!(resp.bucket(), bucket_name, "Bucket mismatch for {:?}", algo);
        assert_eq!(resp.object(), dst_object, "Object mismatch for {:?}", algo);

        // Cleanup
        ctx.client
            .delete_object(&bucket_name, &src_object)
            .build()
            .send()
            .await
            .unwrap();
        ctx.client
            .delete_object(&bucket_name, &dst_object)
            .build()
            .send()
            .await
            .unwrap();
    }
}

/// Test ComposeObject with multiple sources (multipart compose)
/// Note: Multi-source compose uses multipart copy which requires 5MB+ per source part
/// Checksum verification on multipart copy requires source objects to have checksums stored,
/// which is complex with streaming uploads. This test validates the basic multipart compose works.
#[minio_macros::test]
async fn compose_object_multiple_sources(ctx: TestContext, bucket_name: String) {
    let src_object1 = rand_object_name();
    let src_object2 = rand_object_name();
    let dst_object = rand_object_name();

    // Each source must be at least 5MB for multipart copy (except last part)
    let size1: u64 = 5 * 1024 * 1024; // 5MB
    let size2: u64 = 1024; // 1KB for final part (can be smaller)

    // Create source objects
    let content1 = ObjectContent::new_from_stream(RandSrc::new(size1), Some(size1));
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &src_object1, content1)
        .build()
        .send()
        .await
        .unwrap();

    let content2 = ObjectContent::new_from_stream(RandSrc::new(size2), Some(size2));
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &src_object2, content2)
        .build()
        .send()
        .await
        .unwrap();

    // Compose multiple sources
    let sources = vec![
        ComposeSource::new(&bucket_name, &src_object1).unwrap(),
        ComposeSource::new(&bucket_name, &src_object2).unwrap(),
    ];
    let resp: ComposeObjectResponse = ctx
        .client
        .compose_object(&bucket_name, &dst_object, sources)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), dst_object);

    // Verify the composed object size
    let stat_resp = ctx
        .client
        .stat_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(stat_resp.size().unwrap(), size1 + size2);

    // Cleanup
    ctx.client
        .delete_object(&bucket_name, &src_object1)
        .build()
        .send()
        .await
        .unwrap();
    ctx.client
        .delete_object(&bucket_name, &src_object2)
        .build()
        .send()
        .await
        .unwrap();
    ctx.client
        .delete_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();
}

// ============================================================================
// Trailing Checksum tests
// ============================================================================

/// Helper function to upload an object with a trailing checksum
async fn upload_with_trailing_checksum(
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
        .use_trailing_checksum(true)
        .build();

    PutObject::builder()
        .inner(inner)
        .build()
        .send()
        .await
        .unwrap()
}

/// Test uploading an object with trailing CRC32 checksum
#[minio_macros::test]
async fn upload_with_trailing_crc32_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing trailing CRC32 checksum.";

    let resp = upload_with_trailing_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        data,
        ChecksumAlgorithm::CRC32,
    )
    .await;

    assert_eq!(resp.bucket(), bucket_name.as_str());
    assert_eq!(resp.object(), object_name.as_str());

    // Verify we can download the object
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content().unwrap();
    let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), data);

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test uploading an object with trailing CRC32C checksum
#[minio_macros::test]
async fn upload_with_trailing_crc32c_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing trailing CRC32C checksum.";

    let resp = upload_with_trailing_checksum(
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

/// Test uploading an object with trailing CRC64NVME checksum
#[minio_macros::test]
async fn upload_with_trailing_crc64nvme_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing trailing CRC64NVME checksum.";

    let resp = upload_with_trailing_checksum(
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

/// Test uploading an object with trailing SHA1 checksum
#[minio_macros::test]
async fn upload_with_trailing_sha1_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing trailing SHA1 checksum.";

    let resp = upload_with_trailing_checksum(
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

/// Test uploading an object with trailing SHA256 checksum
#[minio_macros::test]
async fn upload_with_trailing_sha256_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let data = b"Hello, MinIO! Testing trailing SHA256 checksum.";

    let resp = upload_with_trailing_checksum(
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

/// Test round-trip with trailing CRC64NVME checksum (the new default in MinIO)
#[minio_macros::test]
async fn upload_download_with_trailing_crc64nvme_verification(
    ctx: TestContext,
    bucket_name: String,
) {
    let object_name = rand_object_name();
    let data = b"Round-trip test with trailing CRC64NVME checksum verification.";

    upload_with_trailing_checksum(
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
    // Server may or may not return checksums depending on configuration
    if let Some(algo) = algorithm {
        assert_eq!(algo, ChecksumAlgorithm::CRC64NVME);
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(downloaded.as_ref(), data);
    } else {
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

/// Test all checksum algorithms with trailing checksums
#[minio_macros::test]
async fn test_all_trailing_checksum_algorithms(ctx: TestContext, bucket_name: String) {
    let algorithms = vec![
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ];

    for algo in algorithms {
        let object_name = format!("trailing-checksum-test-{:?}-{}", algo, rand_object_name());
        let data = format!("Testing trailing {:?} checksum algorithm", algo);

        upload_with_trailing_checksum(&ctx, &bucket_name, &object_name, data.as_bytes(), algo)
            .await;

        let get_resp = ctx
            .client
            .get_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();

        let detected_algo = get_resp.detect_checksum_algorithm();
        if let Some(detected) = detected_algo {
            assert_eq!(detected, algo, "Algorithm mismatch for trailing {:?}", algo);
            let downloaded = get_resp.content_verified().await.unwrap();
            assert_eq!(downloaded.as_ref(), data.as_bytes());
        } else {
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

/// Test trailing checksum with larger data to exercise chunked encoding.
///
/// NOTE: This test requires a newer MinIO server that supports trailing checksums.
/// Older servers may fail with "IncompleteBody" errors.
/// Run with `cargo test -- --ignored` to include this test.
#[minio_macros::test(ignore = "Requires newer MinIO server with trailing checksum support")]
async fn upload_download_large_data_with_trailing_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    // Use 100KB which is larger than the 64KB default chunk size
    let data = vec![0xAB; 1024 * 100];

    upload_with_trailing_checksum(
        &ctx,
        &bucket_name,
        &object_name,
        &data,
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

/// Test PutObjectContent with trailing checksums
#[minio_macros::test]
async fn put_object_content_with_trailing_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    let content = "Testing PutObjectContent with trailing CRC64NVME checksum.";

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, content)
        .checksum_algorithm(ChecksumAlgorithm::CRC64NVME)
        .use_trailing_checksum(true)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);

    // Verify the object
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content().unwrap();
    let bytes = downloaded.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), content.as_bytes());

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

// ============================================================================
// Multipart Upload Checksum tests
// ============================================================================
// These tests verify that multipart uploads with checksums work correctly,
// including the handling of COMPOSITE checksums on download.

/// Test multipart upload with CRC32C checksum and verify download works.
///
/// This test uploads an object larger than 5MB to trigger multipart upload,
/// with checksums enabled. The resulting object will have a COMPOSITE checksum
/// (checksum-of-checksums) which cannot be verified by computing a hash over
/// the full object. The test verifies that:
/// 1. Upload succeeds with checksums
/// 2. Download works without checksum verification errors
/// 3. Content is correct
#[minio_macros::test]
async fn multipart_upload_with_checksum_crc32c(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    // 6MB to ensure multipart upload (threshold is 5MB)
    let size: u64 = 6 * 1024 * 1024;

    let content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, content)
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);

    // Download and verify - should work even with composite checksum
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    // Check if composite checksum is detected (if server returns checksums)
    let has_composite = get_resp.has_composite_checksum();
    let algorithm = get_resp.detect_checksum_algorithm();

    // content_verified() should work without error (skips verification for composite)
    let downloaded = get_resp.content_verified().await.unwrap();
    assert_eq!(downloaded.len(), size as usize);

    // Log for debugging
    if algorithm.is_some() {
        log::info!(
            "Multipart object has checksum algorithm: {:?}, composite: {}",
            algorithm,
            has_composite
        );
    }

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test multipart upload with CRC64NVME checksum (the recommended algorithm).
#[minio_macros::test]
async fn multipart_upload_with_checksum_crc64nvme(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    // 6MB to ensure multipart upload
    let size: u64 = 6 * 1024 * 1024;

    let content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, content)
        .checksum_algorithm(ChecksumAlgorithm::CRC64NVME)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);

    // Download with streaming verification (should skip for composite)
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content().unwrap();
    let bytes = downloaded.to_segmented_bytes().await.unwrap();
    assert_eq!(bytes.len(), size as usize);

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test multipart upload with trailing checksums.
///
/// NOTE: This test requires a newer MinIO server that supports trailing checksums
/// with multipart uploads. Older servers may fail with "IncompleteBody" errors.
/// Run with `cargo test -- --ignored` to include this test.
#[minio_macros::test(
    ignore = "Requires newer MinIO server with trailing checksum + multipart support"
)]
async fn multipart_upload_with_trailing_checksum(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();
    // 6MB to ensure multipart upload
    let size: u64 = 6 * 1024 * 1024;

    let content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &object_name, content)
        .checksum_algorithm(ChecksumAlgorithm::CRC64NVME)
        .use_trailing_checksum(true)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);

    // Download and verify content
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();

    let downloaded = get_resp.content_verified().await.unwrap();
    assert_eq!(downloaded.len(), size as usize);

    ctx.client
        .delete_object(&bucket_name, &object_name)
        .build()
        .send()
        .await
        .unwrap();
}

/// Test all checksum algorithms with multipart upload.
#[minio_macros::test]
async fn multipart_upload_all_checksum_algorithms(ctx: TestContext, bucket_name: String) {
    let algorithms = vec![
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ];

    // 6MB to ensure multipart upload
    let size: u64 = 6 * 1024 * 1024;

    for algo in algorithms {
        let object_name = format!("multipart-{:?}-{}", algo, rand_object_name());

        let content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));
        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &object_name, content)
            .checksum_algorithm(algo)
            .build()
            .send()
            .await
            .unwrap();

        assert_eq!(resp.bucket(), bucket_name, "Bucket mismatch for {:?}", algo);

        // Download and verify - should work for all algorithms
        let get_resp = ctx
            .client
            .get_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();

        // content_verified should work (skips for composite)
        let downloaded = get_resp.content_verified().await.unwrap();
        assert_eq!(
            downloaded.len(),
            size as usize,
            "Size mismatch for {:?}",
            algo
        );

        ctx.client
            .delete_object(&bucket_name, &object_name)
            .build()
            .send()
            .await
            .unwrap();
    }
}

/// Test compose with multiple sources and checksums (creates multipart with composite checksum).
///
/// NOTE: This test requires a newer MinIO server that supports compose operations
/// with checksum verification. Older servers may fail because they don't properly
/// store/return checksums on source objects needed for compose validation.
/// Run with `cargo test -- --ignored` to include this test.
#[minio_macros::test(ignore = "Requires newer MinIO server with compose + checksum support")]
async fn compose_multiple_sources_with_checksum(ctx: TestContext, bucket_name: String) {
    let src_object1 = rand_object_name();
    let src_object2 = rand_object_name();
    let dst_object = rand_object_name();

    // Each source must be at least 5MB for multipart copy (except last part)
    let size1: u64 = 5 * 1024 * 1024; // 5MB
    let size2: u64 = 1024; // 1KB for final part

    // Create source objects with checksums
    let content1 = ObjectContent::new_from_stream(RandSrc::new(size1), Some(size1));
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &src_object1, content1)
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    let content2 = ObjectContent::new_from_stream(RandSrc::new(size2), Some(size2));
    let _resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(&bucket_name, &src_object2, content2)
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    // Compose multiple sources with checksum
    let sources = vec![
        ComposeSource::new(&bucket_name, &src_object1).unwrap(),
        ComposeSource::new(&bucket_name, &src_object2).unwrap(),
    ];
    let resp: ComposeObjectResponse = ctx
        .client
        .compose_object(&bucket_name, &dst_object, sources)
        .checksum_algorithm(ChecksumAlgorithm::CRC32C)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), dst_object);

    // Download and verify - composite checksum handling
    let get_resp = ctx
        .client
        .get_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();

    // Should work even with composite checksum
    let downloaded = get_resp.content_verified().await.unwrap();
    assert_eq!(downloaded.len(), (size1 + size2) as usize);

    // Cleanup
    ctx.client
        .delete_object(&bucket_name, &src_object1)
        .build()
        .send()
        .await
        .unwrap();
    ctx.client
        .delete_object(&bucket_name, &src_object2)
        .build()
        .send()
        .await
        .unwrap();
    ctx.client
        .delete_object(&bucket_name, &dst_object)
        .build()
        .send()
        .await
        .unwrap();
}
