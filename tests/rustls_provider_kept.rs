// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Runs in its own process, so the provider installed here is the first one.
#![cfg(feature = "rustls-tls")]

use std::sync::Arc;

use minio::s3::MinioClientBuilder;
use minio::s3::http::BaseUrl;
use rustls::crypto::CryptoProvider;

#[test]
fn building_a_client_keeps_the_application_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    let installed = Arc::clone(CryptoProvider::get_default().unwrap());
    let base_url: BaseUrl = "https://play.min.io".parse().unwrap();
    MinioClientBuilder::new(base_url).build().unwrap();
    assert!(Arc::ptr_eq(
        &installed,
        CryptoProvider::get_default().unwrap()
    ));
}
