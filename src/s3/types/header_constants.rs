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

pub const IF_MATCH: &str = "If-Match";
pub const IF_NONE_MATCH: &str = "If-None-Match";
pub const IF_MODIFIED_SINCE: &str = "If-Modified-Since";
pub const IF_UNMODIFIED_SINCE: &str = "If-Unmodified-Since";
pub const CONTENT_MD5: &str = "Content-MD5";
pub const CONTENT_TYPE: &str = "Content-Type";
pub const AUTHORIZATION: &str = "Authorization";
pub const RANGE: &str = "Range";
pub const HOST: &str = "Host";
pub const CONTENT_LENGTH: &str = "Content-Length";

pub const POLICY: &str = "policy";

pub const X_MINIO_DEPLOYMENT_ID: &str = "X-Minio-Deployment-Id";

pub const X_AMZ_VERSION_ID: &str = "X-Amz-Version-Id";
pub const X_AMZ_ID_2: &str = "X-Amz-Id-2";
pub const X_AMZ_WRITE_OFFSET_BYTES: &str = "X-Amz-Write-Offset-Bytes";

pub const X_AMZ_OBJECT_SIZE: &str = "X-Amz-Object-Size";
pub const X_AMZ_TAGGING: &str = "X-Amz-Tagging";

pub const X_AMZ_BUCKET_REGION: &str = "X-Amz-Bucket-Region";

pub const X_AMZ_OBJECT_LOCK_MODE: &str = "X-Amz-Object-Lock-Mode";

pub const X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "X-Amz-Object-Lock-Retain-Until-Date";

pub const X_AMZ_OBJECT_LOCK_LEGAL_HOLD: &str = "X-Amz-Object-Lock-Legal-Hold";

pub const X_AMZ_METADATA_DIRECTIVE: &str = "X-Amz-Metadata-Directive";

pub const X_AMZ_TAGGING_DIRECTIVE: &str = "X-Amz-Tagging-Directive";

pub const X_AMZ_COPY_SOURCE: &str = "X-Amz-Copy-Source";

pub const X_AMZ_COPY_SOURCE_RANGE: &str = "X-Amz-Copy-Source-Range";

pub const X_AMZ_COPY_SOURCE_IF_MATCH: &str = "X-Amz-Copy-Source-If-Match";

pub const X_AMZ_COPY_SOURCE_IF_NONE_MATCH: &str = "X-Amz-Copy-Source-If-None-Match";

pub const X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE: &str = "X-Amz-Copy-Source-If-Unmodified-Since";

pub const X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE: &str = "X-Amz-Copy-Source-If-Modified-Since";

pub const X_AMZ_BUCKET_OBJECT_LOCK_ENABLED: &str = "X-Amz-Bucket-Object-Lock-Enabled";

pub const X_AMZ_BYPASS_GOVERNANCE_RETENTION: &str = "X-Amz-Bypass-Governance-Retention";

pub const X_AMZ_DATE: &str = "X-Amz-Date";

pub const X_AMZ_DELETE_MARKER: &str = "X-Amz-Delete-Marker";
pub const X_AMZ_ALGORITHM: &str = "X-Amz-Algorithm";

pub const X_AMZ_CREDENTIAL: &str = "X-Amz-Credential";

pub const X_AMZ_SIGNATURE: &str = "X-Amz-Signature";

pub const X_AMZ_REQUEST_ID: &str = "X-Amz-Request-Id";

pub const X_AMZ_EXPIRES: &str = "X-Amz-Expires";

pub const X_AMZ_SIGNED_HEADERS: &str = "X-Amz-SignedHeaders";

pub const X_AMZ_CONTENT_SHA256: &str = "X-Amz-Content-SHA256";

pub const X_AMZ_SECURITY_TOKEN: &str = "X-Amz-Security-Token";

pub const X_AMZ_SERVER_SIDE_ENCRYPTION: &str = "X-Amz-Server-Side-Encryption";

pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT: &str = "X-Amz-Server-Side-Encryption-Context";

pub const X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: &str =
    "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id";

pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
    "X-Amz-Server-Side-Encryption-Customer-Algorithm";

pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str =
    "X-Amz-Server-Side-Encryption-Customer-Key";
pub const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
    "X-Amz-Server-Side-Encryption-Customer-Key-MD5";

pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm";

pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key";

pub const X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-MD5";

pub const X_AMZ_CHECKSUM_ALGORITHM: &str = "X-Amz-Checksum-Algorithm";

pub const X_AMZ_CHECKSUM_CRC32: &str = "X-Amz-Checksum-CRC32";

pub const X_AMZ_CHECKSUM_CRC32C: &str = "X-Amz-Checksum-CRC32C";

pub const X_AMZ_CHECKSUM_SHA1: &str = "X-Amz-Checksum-SHA1";

pub const X_AMZ_CHECKSUM_SHA256: &str = "X-Amz-Checksum-SHA256";

pub const X_AMZ_CHECKSUM_CRC64NVME: &str = "X-Amz-Checksum-CRC64NVME";

pub const X_AMZ_CHECKSUM_TYPE: &str = "X-Amz-Checksum-Type";

pub const X_AMZ_TRAILER: &str = "X-Amz-Trailer";

pub const X_AMZ_DECODED_CONTENT_LENGTH: &str = "X-Amz-Decoded-Content-Length";

pub const CONTENT_ENCODING: &str = "Content-Encoding";

/// Content-SHA256 value for streaming uploads with unsigned payload and trailing checksum
pub const STREAMING_UNSIGNED_PAYLOAD_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

/// Content-SHA256 value for streaming uploads with signed payload and trailing checksum.
/// Each chunk is signed with AWS Signature V4, and the trailer includes a trailer signature.
pub const STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER: &str =
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

// Iceberg REST API headers
pub const IDEMPOTENCY_KEY: &str = "Idempotency-Key";
pub const X_ICEBERG_ACCESS_DELEGATION: &str = "X-Iceberg-Access-Delegation";

// Iceberg REST API query parameters
pub const PAGE_TOKEN: &str = "pageToken";
pub const PAGE_SIZE: &str = "pageSize";
pub const PARENT: &str = "parent";
pub const PURGE_REQUESTED: &str = "purgeRequested";
pub const SNAPSHOTS: &str = "snapshots";
pub const PLAN_ID: &str = "planId";

// MinIO-specific query parameters
pub const PRESERVE_BUCKET: &str = "preserve-bucket";
pub const FORCE: &str = "force";

// MinIO S3Tables-specific headers
pub const X_MINIO_SIMD_MODE: &str = "X-Minio-Simd-Mode";
pub const X_MINIO_STORAGE_PUSHDOWN: &str = "X-Minio-Storage-Pushdown";
