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

use minio::s3::utils::{
    ChecksumAlgorithm, compute_checksum, crc32_checksum, crc32c, crc64nvme_checksum, sha1_hash,
    sha256_checksum,
};

/// Test CRC32 checksum computation
#[test]
fn test_crc32_checksum() {
    let data = b"Hello, World!";
    let checksum = crc32_checksum(data);

    // Verify it's base64 encoded
    assert!(!checksum.is_empty());
    assert!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &checksum).is_ok());
}

/// Test CRC32C checksum computation
#[test]
fn test_crc32c_checksum() {
    let data = b"Hello, World!";
    let checksum = crc32c(data);

    // Verify it's base64 encoded
    assert!(!checksum.is_empty());
    assert!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &checksum).is_ok());
}

/// Test CRC64-NVME checksum computation
#[test]
fn test_crc64nvme_checksum() {
    let data = b"Hello, World!";
    let checksum = crc64nvme_checksum(data);

    // Verify it's base64 encoded
    assert!(!checksum.is_empty());
    assert!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &checksum).is_ok());

    // Verify it's different from CRC32/CRC32C (different algorithms produce different results)
    let crc32_result = crc32_checksum(data);
    assert_ne!(checksum, crc32_result);
}

/// Test SHA1 hash computation
#[test]
fn test_sha1_hash() {
    let data = b"Hello, World!";
    let hash = sha1_hash(data);

    // Verify it's base64 encoded
    assert!(!hash.is_empty());
    assert!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &hash).is_ok());
}

/// Test SHA256 checksum computation
#[test]
fn test_sha256_checksum() {
    let data = b"Hello, World!";
    let checksum = sha256_checksum(data);

    // Verify it's base64 encoded
    assert!(!checksum.is_empty());
    assert!(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &checksum).is_ok());
}

/// Test compute_checksum with all algorithms
#[test]
fn test_compute_checksum_all_algorithms() {
    let data = b"Test data for checksums";

    let crc32 = compute_checksum(ChecksumAlgorithm::CRC32, data);
    let crc32c = compute_checksum(ChecksumAlgorithm::CRC32C, data);
    let crc64nvme = compute_checksum(ChecksumAlgorithm::CRC64NVME, data);
    let sha1 = compute_checksum(ChecksumAlgorithm::SHA1, data);
    let sha256 = compute_checksum(ChecksumAlgorithm::SHA256, data);

    // All should be non-empty and valid base64
    for checksum in [&crc32, &crc32c, &crc64nvme, &sha1, &sha256] {
        assert!(!checksum.is_empty());
        assert!(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, checksum).is_ok()
        );
    }

    // All should be different (different algorithms)
    assert_ne!(crc32, crc32c);
    assert_ne!(crc32, crc64nvme);
    assert_ne!(crc32, sha1);
    assert_ne!(crc32, sha256);
    assert_ne!(crc32c, crc64nvme);
}

/// Test that different data produces different checksums
#[test]
fn test_different_data_different_checksums() {
    let data1 = b"First test data";
    let data2 = b"Second test data";

    // Test with each algorithm
    for algorithm in [
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ] {
        let checksum1 = compute_checksum(algorithm, data1);
        let checksum2 = compute_checksum(algorithm, data2);
        assert_ne!(
            checksum1, checksum2,
            "Algorithm {:?} produced same checksum for different data",
            algorithm
        );
    }
}

/// Test that same data produces same checksums (deterministic)
#[test]
fn test_deterministic_checksums() {
    let data = b"Deterministic test data";

    for algorithm in [
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ] {
        let checksum1 = compute_checksum(algorithm, data);
        let checksum2 = compute_checksum(algorithm, data);
        assert_eq!(
            checksum1, checksum2,
            "Algorithm {:?} is not deterministic",
            algorithm
        );
    }
}

/// Test empty data checksums
#[test]
fn test_empty_data_checksums() {
    let data = b"";

    for algorithm in [
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ] {
        let checksum = compute_checksum(algorithm, data);
        // Empty data should still produce a valid checksum
        assert!(!checksum.is_empty());
        assert!(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &checksum).is_ok()
        );
    }
}

/// Test large data checksums
#[test]
fn test_large_data_checksums() {
    // Test with 1MB of data
    let data = vec![0x42u8; 1024 * 1024];

    for algorithm in [
        ChecksumAlgorithm::CRC32,
        ChecksumAlgorithm::CRC32C,
        ChecksumAlgorithm::CRC64NVME,
        ChecksumAlgorithm::SHA1,
        ChecksumAlgorithm::SHA256,
    ] {
        let checksum = compute_checksum(algorithm, &data);
        assert!(!checksum.is_empty());
        assert!(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &checksum).is_ok()
        );
    }
}

/// Test ChecksumAlgorithm::as_str()
#[test]
fn test_checksum_algorithm_as_str() {
    assert_eq!(ChecksumAlgorithm::CRC32.as_str(), "CRC32");
    assert_eq!(ChecksumAlgorithm::CRC32C.as_str(), "CRC32C");
    assert_eq!(ChecksumAlgorithm::CRC64NVME.as_str(), "CRC64NVME");
    assert_eq!(ChecksumAlgorithm::SHA1.as_str(), "SHA1");
    assert_eq!(ChecksumAlgorithm::SHA256.as_str(), "SHA256");
}

/// Test ChecksumAlgorithm::from_str()
#[test]
fn test_checksum_algorithm_from_str() {
    use std::str::FromStr;

    // Test uppercase
    assert_eq!(
        ChecksumAlgorithm::from_str("CRC32").unwrap(),
        ChecksumAlgorithm::CRC32
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("CRC32C").unwrap(),
        ChecksumAlgorithm::CRC32C
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("CRC64NVME").unwrap(),
        ChecksumAlgorithm::CRC64NVME
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("SHA1").unwrap(),
        ChecksumAlgorithm::SHA1
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("SHA256").unwrap(),
        ChecksumAlgorithm::SHA256
    );

    // Test lowercase
    assert_eq!(
        ChecksumAlgorithm::from_str("crc32").unwrap(),
        ChecksumAlgorithm::CRC32
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("crc32c").unwrap(),
        ChecksumAlgorithm::CRC32C
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("crc64nvme").unwrap(),
        ChecksumAlgorithm::CRC64NVME
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("sha1").unwrap(),
        ChecksumAlgorithm::SHA1
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("sha256").unwrap(),
        ChecksumAlgorithm::SHA256
    );

    // Test mixed case
    assert_eq!(
        ChecksumAlgorithm::from_str("Crc32").unwrap(),
        ChecksumAlgorithm::CRC32
    );
    assert_eq!(
        ChecksumAlgorithm::from_str("Sha256").unwrap(),
        ChecksumAlgorithm::SHA256
    );

    // Test invalid
    assert!(ChecksumAlgorithm::from_str("INVALID").is_err());
    assert!(ChecksumAlgorithm::from_str("MD5").is_err());
}
