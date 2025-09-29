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

use async_std::io::ReadExt;
use minio::s3::builders::ObjectContent;
use minio::s3::response::a_response_traits::{HasBucket, HasObject};
use minio::s3::response::{GetObjectResponse, PutObjectContentResponse};
use minio::s3::types::S3Api;
use minio::s3::utils::hex_encode;
use minio_common::rand_reader::RandReader;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name_utf8;
#[cfg(feature = "ring")]
use ring::digest::{Context, SHA256};
#[cfg(not(feature = "ring"))]
use sha2::{Digest, Sha256};
use std::path::PathBuf;

async fn get_hash(filename: &str) -> String {
    #[cfg(feature = "ring")]
    {
        let mut context = Context::new(&SHA256);
        let mut file = async_std::fs::File::open(filename).await.unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        context.update(&buf);
        hex_encode(context.finish().as_ref())
    }
    #[cfg(not(feature = "ring"))]
    {
        let mut hasher = Sha256::new();
        let mut file = async_std::fs::File::open(filename).await.unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        hasher.update(&buf);
        hex_encode(hasher.finalize().as_slice())
    }
}

async fn test_upload_download_object(
    ctx: &TestContext,
    bucket_name: &str,
    object_name: &str,
    size: u64,
) {
    let mut file = async_std::fs::File::create(&object_name).await.unwrap();

    async_std::io::copy(&mut RandReader::new(size), &mut file)
        .await
        .unwrap();

    file.sync_all().await.unwrap();

    let obj: ObjectContent = PathBuf::from(object_name).as_path().into();

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(bucket_name, object_name, obj)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);
    assert_eq!(resp.object_size(), size);

    let filename: String = rand_object_name_utf8(20);
    let resp: GetObjectResponse = ctx
        .client
        .get_object(bucket_name, object_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);

    // save the object to a file
    resp.content()
        .unwrap()
        .to_file(PathBuf::from(&filename).as_path())
        .await
        .unwrap();
    assert_eq!(get_hash(object_name).await, get_hash(&filename).await);

    async_std::fs::remove_file(&object_name).await.unwrap();
    async_std::fs::remove_file(&filename).await.unwrap();
}

/// Test uploading and downloading an object with a size that fits in a single part
#[minio_macros::test]
async fn upload_download_object_1(ctx: TestContext, bucket_name: String) {
    test_upload_download_object(&ctx, &bucket_name, &rand_object_name_utf8(20), 16).await;
}

/// Test uploading and downloading an object with a name that contains white space characters.
#[minio_macros::test]
async fn upload_download_object_2(ctx: TestContext, bucket_name: String) {
    test_upload_download_object(&ctx, &bucket_name, "a b+c", 16).await;
}

/// Test uploading and downloading an object with a size that needs multiple parts.
#[minio_macros::test]
async fn upload_download_object_3(ctx: TestContext, bucket_name: String) {
    test_upload_download_object(
        &ctx,
        &bucket_name,
        &rand_object_name_utf8(20),
        16 + 5 * 1024 * 1024,
    )
    .await;
}
