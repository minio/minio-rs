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

pub const IF_MATCH: &str = "if-match";
pub const IF_NONE_MATCH: &str = "if-none-match";
pub const IF_MODIFIED_SINCE: &str = "if-modified-since";
pub const IF_UNMODIFIED_SINCE: &str = "if-unmodified-since";
pub const CONTENT_MD5: &str = "Content-MD5";
pub const CONTENT_TYPE: &str = "Content-Type";
pub const AUTHORIZATION: &str = "Authorization";
pub const RANGE: &str = "Range";
pub const HOST: &str = "Host";
pub const CONTENT_LENGTH: &str = "Content-Length";

pub const POLICY: &str = "policy";

pub const X_MINIO_DEPLOYMENT_ID: &str = "x-minio-deployment-id";

pub const X_AMZ_VERSION_ID: &str = "x-amz-version-id";
pub const X_AMZ_ID_2: &str = "x-amz-id-2";
pub const X_AMZ_WRITE_OFFSET_BYTES: &str = "x-amz-write-offset-bytes";

pub const X_AMZ_OBJECT_SIZE: &str = "x-amz-object-size";
pub const X_AMZ_TAGGING: &str = "x-amz-tagging";

pub const X_AMZ_BUCKET_REGION: &str = "x-amz-bucket-region";

pub const X_AMZ_OBJECT_LOCK_MODE: &str = "x-amz-object-lock-mode";

pub const X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "x-amz-object-lock-retain-until-date";

pub const X_AMZ_OBJECT_LOCK_LEGAL_HOLD: &str = "x-amz-object-lock-legal-hold";

pub const X_AMZ_METADATA_DIRECTIVE: &str = "x-amz-metadata-directive";

pub const X_AMZ_TAGGING_DIRECTIVE: &str = "x-amz-tagging-directive";

pub const X_AMZ_COPY_SOURCE: &str = "x-amz-copy-source";

pub const X_AMZ_COPY_SOURCE_RANGE: &str = "x-amz-copy-source-range";

pub const X_AMZ_COPY_SOURCE_IF_MATCH: &str = "x-amz-copy-source-if-match";

pub const X_AMZ_COPY_SOURCE_IF_NONE_MATCH: &str = "x-amz-copy-source-if-none-match";

pub const X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE: &str = "x-amz-copy-source-if-unmodified-since";

pub const X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE: &str = "x-amz-copy-source-if-modified-since";

pub const X_AMZ_BUCKET_OBJECT_LOCK_ENABLED: &str = "x-amz-bucket-object-lock-enabled";

pub const X_AMZ_BYPASS_GOVERNANCE_RETENTION: &str = "x-amz-bypass-governance-retention";

pub const X_AMZ_DATE: &str = "x-amz-date";

pub const X_AMZ_DELETE_MARKER: &str = "x-amz-delete-marker";
pub const X_AMZ_ALGORITHM: &str = "x-amz-algorithm";

pub const X_AMZ_CREDENTIAL: &str = "x-amz-credential";

pub const X_AMZ_SIGNATURE: &str = "x-amz-signature";

pub const X_AMZ_REQUEST_ID: &str = "x-amz-request-id";

pub const X_AMZ_EXPIRES: &str = "x-amz-expires";

pub const X_AMZ_SIGNED_HEADERS: &str = "x-amz-signedheaders";

pub const X_AMZ_CONTENT_SHA256: &str = "x-amz-content-sha256";

pub const X_AMZ_SECURITY_TOKEN: &str = "x-amz-security-token";

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
