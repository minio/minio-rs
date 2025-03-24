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
use rand::distributions::{Alphanumeric, DistString};

pub fn rand_bucket_name() -> String {
    Alphanumeric
        .sample_string(&mut rand::thread_rng(), 8)
        .to_lowercase()
}

pub fn rand_object_name() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), 8)
}

pub async fn get_bytes_from_response(v: Result<reqwest::Response, Error>) -> bytes::Bytes {
    match v {
        Ok(r) => match r.bytes().await {
            Ok(b) => b,
            Err(e) => panic!("{:?}", e),
        },
        Err(e) => panic!("{:?}", e),
    }
}

pub fn get_response_from_bytes(bytes: bytes::Bytes) -> reqwest::Response {
    let http_response = HttpResponse::builder()
        .status(StatusCode::OK) // You can customize the status if needed
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .expect("Failed to build HTTP response");

    reqwest::Response::try_from(http_response).expect("Failed to convert to reqwest::Response")
}
