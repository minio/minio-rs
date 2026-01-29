// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Encryption types for S3 Tables encryption operations

use serde::{Deserialize, Serialize};

/// Server-side encryption algorithm
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SseAlgorithm {
    /// S3-managed encryption (SSE-S3)
    #[serde(rename = "AES256")]
    Aes256,
    /// KMS-managed encryption (SSE-KMS)
    #[serde(rename = "aws:kms")]
    AwsKms,
}

impl Default for SseAlgorithm {
    fn default() -> Self {
        Self::Aes256
    }
}

/// Encryption configuration for a warehouse or table
///
/// # Example
///
/// ```
/// use minio::s3tables::types::EncryptionConfiguration;
///
/// // Create S3-managed encryption (default)
/// let s3_encryption = EncryptionConfiguration::s3_managed();
///
/// // Create KMS-managed encryption
/// let kms_encryption = EncryptionConfiguration::kms_managed(
///     "arn:aws:kms:us-east-1:123456789012:key/my-key-id"
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptionConfiguration {
    #[serde(rename = "sseAlgorithm")]
    sse_algorithm: SseAlgorithm,
    #[serde(rename = "kmsKeyArn", skip_serializing_if = "Option::is_none")]
    kms_key_arn: Option<String>,
}

impl EncryptionConfiguration {
    /// Create S3-managed encryption (SSE-S3)
    ///
    /// Uses AES-256 encryption managed by S3.
    pub fn s3_managed() -> Self {
        Self {
            sse_algorithm: SseAlgorithm::Aes256,
            kms_key_arn: None,
        }
    }

    /// Create KMS-managed encryption (SSE-KMS)
    ///
    /// Uses AWS KMS for encryption key management.
    ///
    /// # Arguments
    ///
    /// * `kms_key_arn` - The ARN of the KMS key to use for encryption
    pub fn kms_managed(kms_key_arn: impl Into<String>) -> Self {
        Self {
            sse_algorithm: SseAlgorithm::AwsKms,
            kms_key_arn: Some(kms_key_arn.into()),
        }
    }

    /// Returns the server-side encryption algorithm
    pub fn sse_algorithm(&self) -> &SseAlgorithm {
        &self.sse_algorithm
    }

    /// Returns the KMS key ARN, if using KMS encryption
    pub fn kms_key_arn(&self) -> Option<&str> {
        self.kms_key_arn.as_deref()
    }
}

impl Default for EncryptionConfiguration {
    fn default() -> Self {
        Self::s3_managed()
    }
}
