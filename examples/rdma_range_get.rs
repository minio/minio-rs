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

//! Puts an object over RDMA, then reads ranges of it back with
//! `rdma_get_object_range` and checks every byte.
//!
//! Reads its settings from the environment, so no secret appears on a
//! command line: `MINIO_ENDPOINT` (such as `http://host:9000`),
//! `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` and `MINIO_BUCKET`, which must exist.

use std::env;
use std::process::ExitCode;

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::rdma::RdmaBuffer;
use minio::s3::types::S3Api;

const SIZE: usize = 8 << 20;
const RANGE: usize = 64 << 10;

struct Aligned {
    ptr: *mut libc::c_void,
    len: usize,
}

impl Aligned {
    fn new(len: usize) -> Self {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut ptr, 4096, len) };
        assert_eq!(rc, 0, "posix_memalign failed");
        unsafe { std::ptr::write_bytes(ptr as *mut u8, 0, len) };
        Self { ptr, len }
    }

    fn bytes(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.len) }
    }

    fn rdma(&self) -> RdmaBuffer {
        unsafe { RdmaBuffer::from_raw(self.ptr, self.len) }
    }
}

impl Drop for Aligned {
    fn drop(&mut self) {
        unsafe { libc::free(self.ptr) };
    }
}

fn pattern(i: usize) -> u8 {
    (i.wrapping_mul(2654435761) >> 13) as u8
}

#[tokio::main]
async fn main() -> ExitCode {
    let var = |k: &str| env::var(k).unwrap_or_else(|_| panic!("set {k}"));
    let (endpoint, bucket) = (var("MINIO_ENDPOINT"), var("MINIO_BUCKET"));
    let base_url: BaseUrl = endpoint.parse().expect("MINIO_ENDPOINT");
    let creds = StaticProvider::new(&var("MINIO_ACCESS_KEY"), &var("MINIO_SECRET_KEY"), None);
    let client = MinioClient::new(base_url, Some(creds), None, None).expect("client");
    if !client.rdma_available() {
        eprintln!("RDMA unavailable: {:?}", client.rdma_unavailable_reason());
        return ExitCode::from(2);
    }

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let object = format!("rdma-range-{nanos:x}-{}", std::process::id());
    let mut src = Aligned::new(SIZE);
    for (i, b) in src.bytes().iter_mut().enumerate() {
        *b = pattern(i);
    }
    let put = client
        .rdma_put_object(bucket.as_str(), object.as_str(), src.rdma())
        .await
        .expect("rdma_put_object");
    assert_eq!(put.bytes_transferred, SIZE, "put moved the wrong count");
    println!("put {SIZE} bytes over RDMA, etag {}", put.etag);

    let mut failed = 0;
    for offset in [0, 4096, (1 << 20) + 123 * 4096, SIZE - RANGE, SIZE - 4096] {
        let mut dst = Aligned::new(RANGE);
        let got = client
            .rdma_get_object_range(bucket.as_str(), object.as_str(), offset as u64, dst.rdma())
            .await
            .expect("rdma_get_object_range");
        let want = RANGE.min(SIZE - offset);
        let bytes = dst.bytes();
        let same = (0..want).all(|i| bytes[i] == pattern(offset + i));
        let ok = got.bytes_transferred == want && same;
        failed += !ok as usize;
        println!(
            "range at {offset:>8}: moved {:>6} of {want:>6} bytes, content {}",
            got.bytes_transferred,
            if same { "matches" } else { "DIFFERS" }
        );
    }

    let mut whole = Aligned::new(SIZE);
    let got = client
        .rdma_get_object(bucket.as_str(), object.as_str(), whole.rdma())
        .await
        .expect("rdma_get_object");
    let same = whole
        .bytes()
        .iter()
        .enumerate()
        .all(|(i, b)| *b == pattern(i));
    failed += !(same && got.bytes_transferred == SIZE) as usize;
    println!(
        "whole object: moved {} bytes, content {}",
        got.bytes_transferred,
        if same { "matches" } else { "DIFFERS" }
    );

    if let Ok(b) = client.delete_object(bucket.as_str(), object.as_str()) {
        let _ = b.build().send().await;
    }
    if failed > 0 {
        eprintln!("{failed} check(s) failed");
        return ExitCode::from(1);
    }
    println!("all checks passed");
    ExitCode::SUCCESS
}
