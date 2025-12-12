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

use http::{Response as HttpResponse, StatusCode};
use minio::s3::error::Error;

use minio::s3::types::{BucketName, ObjectKey};
use rand::Rng;
use rand::distr::StandardUniform;
use uuid::Uuid;

pub fn rand_bucket_name() -> BucketName {
    BucketName::new(format!("test-bucket-{}", Uuid::new_v4())).unwrap()
}

pub fn rand_object_name() -> ObjectKey {
    ObjectKey::new(format!("test-object-{}", Uuid::new_v4())).unwrap()
}

pub fn rand_object_name_utf8(len: usize) -> ObjectKey {
    let rng = rand::rng();
    ObjectKey::new(
        rng.sample_iter(StandardUniform)
            .filter(|c: &char| !c.is_control())
            .take(len)
            .collect::<String>(),
    )
    .unwrap()
}

pub async fn get_bytes_from_response(v: Result<reqwest::Response, Error>) -> bytes::Bytes {
    match v {
        Ok(r) => match r.bytes().await {
            Ok(b) => b,
            Err(e) => panic!("{e:?}"),
        },
        Err(e) => panic!("{e:?}"),
    }
}

pub fn get_response_from_bytes(bytes: bytes::Bytes) -> reqwest::Response {
    let http_response = HttpResponse::builder()
        .status(StatusCode::OK) // You can customize the status if needed
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .expect("Failed to build HTTP response");

    reqwest::Response::from(http_response)
}
