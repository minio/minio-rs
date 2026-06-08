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

use minio::s3::builders::ObjectContent;
use minio::s3::response::{PutObjectContentResponse, UpdateObjectEncryptionResponse};
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{BucketName, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

/// UpdateObjectEncryption is a MinIO (AIStor) extension that rotates the SSE-KMS
/// envelope of an existing object in place.
///
/// NOTE: This requires a KMS-backed AIStor deployment and an object already
/// encrypted with SSE-S3 or SSE-KMS; the `kms_key_arn` must name a key the
/// server knows. Against a server without KMS configured this request will
/// fail, so the test only runs when MINIO_AISTOR is set and assumes the AIStor
/// CI job provides a usable KMS key via UPDATE_OBJECT_ENCRYPTION_KMS_KEY.
#[minio_macros::test]
async fn update_object_encryption(ctx: TestContext, bucket: BucketName) {
    if std::env::var("MINIO_AISTOR").is_err() {
        eprintln!(
            "skipping update_object_encryption: requires AIStor (set MINIO_AISTOR=1)"
        );
        return;
    }

    let kms_key = std::env::var("UPDATE_OBJECT_ENCRYPTION_KMS_KEY")
        .unwrap_or_else(|_| "minio-default-key".to_string());

    let object = rand_object_name();
    let size = 48_u64;

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket,
            &object,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.object_size(), size);

    let resp: UpdateObjectEncryptionResponse = ctx
        .client
        .update_object_encryption(&bucket, &object, kms_key)
        .unwrap()
        .bucket_key_enabled(true)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));
}
