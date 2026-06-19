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
use minio::s3::response::{PutObjectContentResponse, RenameObjectResponse, StatObjectResponse};
use minio::s3::response_traits::{HasBucket, HasObject};
use minio::s3::types::{BucketName, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;

/// RenameObject is a MinIO (AIStor) extension. It renames an object within a
/// single unversioned bucket in one server-side request, then the source key
/// is gone and the destination exists.
#[minio_macros::test]
async fn rename_object(ctx: TestContext, bucket: BucketName) {
    // Source key has a space to exercise rename-source percent-encoding.
    let src = format!("rename src {}.bin", rand_object_name());
    let dst = rand_object_name();
    let size = 32_u64;

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket,
            &src,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.object_size(), size);

    let resp: RenameObjectResponse = ctx
        .client
        .rename_object(&bucket, &src, &dst)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));
    assert_eq!(resp.object(), Some(&dst));

    // The destination now holds the source's data...
    let resp: StatObjectResponse = ctx
        .client
        .stat_object(&bucket, &dst)
        .unwrap()
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.object(), Some(&dst));
    assert_eq!(resp.size().unwrap(), size);

    // ...and the source no longer exists.
    let src_gone = ctx
        .client
        .stat_object(&bucket, &src)
        .unwrap()
        .build()
        .send()
        .await
        .is_err();
    assert!(
        src_gone,
        "source object should no longer exist after rename"
    );
}

/// Recursive prefix rename (MinIO/AIStor extension): `x-amz-rename-recursive: true`
/// renames every object under the source directory prefix into the destination
/// prefix in one server-side request. Source and destination are directory
/// prefixes (trailing `/`); afterwards every child exists under the destination
/// and none remain under the source.
#[minio_macros::test]
async fn rename_prefix(ctx: TestContext, bucket: BucketName) {
    let base = rand_object_name();
    let src_prefix = format!("{base}-src/");
    let dst_prefix = format!("{base}-dst/");
    let children = ["a.bin", "nested/b.bin", "nested/deep/c.bin"];
    let size = 16_u64;

    for child in children {
        let resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(
                &bucket,
                format!("{src_prefix}{child}"),
                ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
            )
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(resp.object_size(), size);
    }

    let resp: RenameObjectResponse = ctx
        .client
        .rename_object(&bucket, &src_prefix, &dst_prefix)
        .unwrap()
        .recursive(true)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), Some(&bucket));

    for child in children {
        // Every child now exists under the destination prefix...
        let moved: StatObjectResponse = ctx
            .client
            .stat_object(&bucket, format!("{dst_prefix}{child}"))
            .unwrap()
            .build()
            .send()
            .await
            .unwrap();
        assert_eq!(moved.size().unwrap(), size);

        // ...and is gone from the source prefix.
        let src_gone = ctx
            .client
            .stat_object(&bucket, format!("{src_prefix}{child}"))
            .unwrap()
            .build()
            .send()
            .await
            .is_err();
        assert!(
            src_gone,
            "source child {child} should be gone after recursive rename"
        );
    }
}
