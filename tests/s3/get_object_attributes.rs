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
use minio::s3::response::{GetObjectAttributesResponse, PutObjectContentResponse};
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{BucketName, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

/// GetObjectAttributes is gated to AIStor because community MinIO may lack it.
/// Verifies the parsed attributes report the object's size and a non-empty ETag.
#[minio_macros::test]
async fn get_object_attributes(ctx: TestContext, bucket: BucketName) {
    let object = rand_object_name();
    let size = 64_u64;

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

    let resp: GetObjectAttributesResponse = ctx
        .client
        .get_object_attributes(&bucket, &object)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&object));

    let attrs = resp.attributes().unwrap();
    assert_eq!(attrs.object_size, size);
    // The body ETag is returned quoted (e.g. "abc..."); only assert presence.
    assert!(
        attrs.etag.is_some(),
        "expected an ETag in object attributes"
    );
}
