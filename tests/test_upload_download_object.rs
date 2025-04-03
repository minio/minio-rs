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

use hex::ToHex;
use minio::s3::response::PutObjectContentResponse;
use minio::s3::types::S3Api;
use minio_common::rand_reader::RandReader;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
#[cfg(feature = "ring")]
use ring::digest::{Context, SHA256};
#[cfg(not(feature = "ring"))]
use sha2::{Digest, Sha256};
#[cfg(feature = "ring")]
use std::io::Read;
use std::path::PathBuf;
use std::{fs, io};
use test_tag::tag;

fn get_hash(filename: &String) -> String {
    #[cfg(feature = "ring")]
    {
        let mut context = Context::new(&SHA256);
        let mut file = fs::File::open(filename).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        context.update(&buf);
        context.finish().encode_hex()
    }
    #[cfg(not(feature = "ring"))]
    {
        let mut hasher = Sha256::new();
        let mut file = fs::File::open(filename).unwrap();
        io::copy(&mut file, &mut hasher).unwrap();
        hasher.finalize().encode_hex()
    }
}

#[tag(s3, s3express)]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn upload_download_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;
    let object_name = rand_object_name();

    let size = 16_u64;
    let mut file = fs::File::create(&object_name).unwrap();
    io::copy(&mut RandReader::new(size), &mut file).unwrap();
    file.sync_all().unwrap();

    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            PathBuf::from(&object_name).as_path(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.object, object_name);
    assert_eq!(resp.object_size, size);

    let filename = rand_object_name();
    let get_obj_rsp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    get_obj_rsp
        .content
        .to_file(PathBuf::from(&filename).as_path())
        .await
        .unwrap();
    assert_eq!(get_hash(&object_name), get_hash(&filename));

    fs::remove_file(&object_name).unwrap();
    fs::remove_file(&filename).unwrap();

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();

    let object_name = rand_object_name();
    let size: u64 = 16 + 5 * 1024 * 1024;
    let mut file = fs::File::create(&object_name).unwrap();
    io::copy(&mut RandReader::new(size), &mut file).unwrap();
    file.sync_all().unwrap();
    ctx.client
        .put_object_content(
            &bucket_name,
            &object_name,
            PathBuf::from(&object_name).as_path(),
        )
        .send()
        .await
        .unwrap();

    let filename = rand_object_name();
    let get_rsp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    get_rsp
        .content
        .to_file(PathBuf::from(&filename).as_path())
        .await
        .unwrap();
    assert_eq!(get_hash(&object_name), get_hash(&filename));

    fs::remove_file(&object_name).unwrap();
    fs::remove_file(&filename).unwrap();
}
