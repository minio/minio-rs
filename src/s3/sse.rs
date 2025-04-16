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

use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::utils::{b64encode, md5sum_hash};
use std::any::Any;

/// Base server side encryption
pub trait Sse: std::fmt::Debug + Send + Sync {
    /// Regular headers
    fn headers(&self) -> Multimap;
    /// Headers for copy operation
    fn copy_headers(&self) -> Multimap;
    fn tls_required(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug)]
/// Server side encryption customer key type
pub struct SseCustomerKey {
    headers: Multimap,
    copy_headers: Multimap,
}

impl SseCustomerKey {
    pub fn new(key: &str) -> Self {
        let b64key: String = b64encode(key);
        let md5key: String = md5sum_hash(key.as_bytes());

        let mut headers = Multimap::with_capacity(3);
        headers.add("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256");
        headers.add("X-Amz-Server-Side-Encryption-Customer-Key", b64key.clone());
        headers.add(
            "X-Amz-Server-Side-Encryption-Customer-Key-MD5",
            md5key.clone(),
        );

        let mut copy_headers = Multimap::with_capacity(3);
        copy_headers.add(
            "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm",
            "AES256",
        );
        copy_headers.add(
            "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key",
            b64key,
        );
        copy_headers.add(
            "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-MD5",
            md5key,
        );

        Self {
            headers,
            copy_headers,
        }
    }
}

impl Sse for SseCustomerKey {
    fn headers(&self) -> Multimap {
        self.headers.clone()
    }

    fn copy_headers(&self) -> Multimap {
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
    headers: Multimap,
}

impl SseKms {
    pub fn new(key: &str, context: Option<&str>) -> SseKms {
        let mut headers = Multimap::with_capacity(3);

        headers.add("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", key);
        headers.add("X-Amz-Server-Side-Encryption", "aws:kms");
        if let Some(v) = context {
            headers.add("X-Amz-Server-Side-Encryption-Context", b64encode(v));
        }

        SseKms { headers }
    }
}

impl Sse for SseKms {
    fn headers(&self) -> Multimap {
        self.headers.clone()
    }

    fn copy_headers(&self) -> Multimap {
        Multimap::with_capacity(0)
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
    headers: Multimap,
}

impl SseS3 {
    pub fn new() -> Self {
        let mut headers = Multimap::new();
        headers.add("X-Amz-Server-Side-Encryption", "AES256");

        Self { headers }
    }
}

impl Default for SseS3 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sse for SseS3 {
    fn headers(&self) -> Multimap {
        self.headers.clone()
    }

    fn copy_headers(&self) -> Multimap {
        Multimap::with_capacity(0)
    }

    fn tls_required(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
