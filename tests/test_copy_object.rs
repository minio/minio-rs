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

mod common;

use crate::common::{create_bucket_helper, rand_object_name, RandReader, TestContext};
use minio::s3::args::{CopyObjectArgs, CopySource, PutObjectArgs, StatObjectArgs};
use minio::s3::types::S3Api;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn copy_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let src_object_name = rand_object_name();

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &src_object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let object_name = rand_object_name();
    ctx.client
        .copy_object(
            &CopyObjectArgs::new(
                &bucket_name,
                &object_name,
                CopySource::new(&bucket_name, &src_object_name).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.size, size);

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    ctx.client
        .remove_object(&bucket_name, src_object_name.as_str())
        .send()
        .await
        .unwrap();
}
