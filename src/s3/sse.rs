// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Server side encryption definitions

use crate::s3::utils;
use std::any::Any;

/// Base server side encryption
pub trait Sse: std::fmt::Debug + Send + Sync {
    fn headers(&self) -> utils::Multimap;
    fn copy_headers(&self) -> utils::Multimap;
    fn tls_required(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug)]
/// Server side encryption customer key type
pub struct SseCustomerKey {
    headers: utils::Multimap,
    copy_headers: utils::Multimap,
}

impl SseCustomerKey {
    pub fn new(key: &str) -> SseCustomerKey {
        let b64key = utils::b64encode(key);
        let md5key = utils::md5sum_hash(key.as_bytes());

        let mut headers = utils::Multimap::new();
        headers.insert(
            String::from("X-Amz-Server-Side-Encryption-Customer-Algorithm"),
            String::from("AES256"),
        );
        headers.insert(
            String::from("X-Amz-Server-Side-Encryption-Customer-Key"),
            b64key.clone(),
        );
        headers.insert(
            String::from("X-Amz-Server-Side-Encryption-Customer-Key-MD5"),
            md5key.clone(),
        );

        let mut copy_headers = utils::Multimap::new();
        copy_headers.insert(
            String::from("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"),
            String::from("AES256"),
        );
        copy_headers.insert(
            String::from("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"),
            b64key,
        );
        copy_headers.insert(
            String::from("X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-MD5"),
            md5key,
        );

        SseCustomerKey {
            headers,
            copy_headers,
        }
    }
}

impl Sse for SseCustomerKey {
    fn headers(&self) -> utils::Multimap {
        self.headers.clone()
    }

    fn copy_headers(&self) -> utils::Multimap {
        self.copy_headers.clone()
    }

    fn tls_required(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
/// Server side encryption KMS type
pub struct SseKms {
    headers: utils::Multimap,
}

impl SseKms {
    pub fn new(key: &str, context: Option<&str>) -> SseKms {
        let mut headers = utils::Multimap::new();
        headers.insert(
            String::from("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"),
            key.to_string(),
        );
        headers.insert(
            String::from("X-Amz-Server-Side-Encryption"),
            String::from("aws:kms"),
        );
        if let Some(v) = context {
            headers.insert(
                String::from("X-Amz-Server-Side-Encryption-Context"),
                utils::b64encode(v),
            );
        }

        SseKms { headers }
    }
}

impl Sse for SseKms {
    fn headers(&self) -> utils::Multimap {
        self.headers.clone()
    }

    fn copy_headers(&self) -> utils::Multimap {
        utils::Multimap::new()
    }

    fn tls_required(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
/// Server side encryption S3 type
pub struct SseS3 {
    headers: utils::Multimap,
}

impl SseS3 {
    pub fn new() -> SseS3 {
        let mut headers = utils::Multimap::new();
        headers.insert(
            String::from("X-Amz-Server-Side-Encryption"),
            String::from("AES256"),
        );

        SseS3 { headers }
    }
}

impl Default for SseS3 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sse for SseS3 {
    fn headers(&self) -> utils::Multimap {
        self.headers.clone()
    }

    fn copy_headers(&self) -> utils::Multimap {
        utils::Multimap::new()
    }

    fn tls_required(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
