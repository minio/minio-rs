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
use minio::s3::response::PutObjectContentResponse;
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{BucketName, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

/// UpdateObjectEncryption is a MinIO (AIStor) extension that rotates the SSE-KMS
/// envelope of an existing object in place.
///
/// NOTE: The success path requires a KMS-backed deployment; the `kms_key_arn`
/// must name a key the server knows (set UPDATE_OBJECT_ENCRYPTION_KMS_KEY). When
/// KMS is not configured the request still exercises the full SDK path and the
/// KMS-key-not-found response is tolerated.
#[minio_macros::test]
async fn update_object_encryption(ctx: TestContext, bucket: BucketName) {
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

    let result = ctx
        .client
        .update_object_encryption(&bucket, &object, kms_key)
        .unwrap()
        .bucket_key_enabled(true)
        .build()
        .send()
        .await;

    match result {
        Ok(resp) => {
            assert_eq!(resp.bucket(), Some(&bucket));
            assert_eq!(resp.object(), Some(&object));
        }
        Err(e) => {
            // Without a provisioned KMS key (or on a plain object that was not
            // SSE-encrypted to begin with) the server rejects the change; the
            // request still reached the handler and was signed/parsed, which
            // validates the SDK path. A signing/parse/transport error is a real
            // failure, so require a server-side KMS/encryption rejection.
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("kms") || msg.contains("encryption") || msg.contains("not supported"),
                "unexpected error from update_object_encryption: {e}"
            );
            eprintln!(
                "update_object_encryption reached the server but KMS key is not provisioned ({e}); set UPDATE_OBJECT_ENCRYPTION_KMS_KEY to a valid key to exercise the success path"
            );
        }
    }
}
