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
use minio::s3::error::Error;
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

    let stat_src: Result<StatObjectResponse, Error> = ctx
        .client
        .stat_object(&bucket, &src)
        .unwrap()
        .build()
        .send()
        .await;
    assert!(
        stat_src.is_err(),
        "source object should no longer exist after rename; got: {stat_src:?}"
    );
}
