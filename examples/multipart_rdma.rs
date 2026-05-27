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
//
// Multipart RDMA upload: split a single host buffer into N page-aligned slices,
// register/upload each as a multipart part over RDMA, then complete the upload.
// Each part's buffer must be at least 5 MiB to satisfy S3 multipart minimums.

use libc::c_void;
use std::env;
use std::process::ExitCode;
use std::ptr;

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::rdma::{RdmaBuffer, RdmaPart, crc64nvme_base64};

const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: {} <server> <access_key> <secret_key> [part_size] [num_parts]",
            args[0]
        );
        eprintln!("  part_size defaults to {MIN_PART_SIZE} (5 MiB), num_parts to 3");
        return ExitCode::from(1);
    }

    let host = &args[1];
    let access_key = &args[2];
    let secret_key = &args[3];
    let part_size: usize = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(MIN_PART_SIZE);
    let num_parts: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(3);

    if part_size < MIN_PART_SIZE {
        eprintln!("part_size {part_size} below S3 minimum {MIN_PART_SIZE}");
        return ExitCode::from(1);
    }

    let base_url: BaseUrl = match host.parse() {
        Ok(u) => u,
        Err(e) => {
            eprintln!("invalid endpoint {host}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match MinioClient::new(
        base_url,
        Some(StaticProvider::new(access_key, secret_key, None)),
        None,
        None,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("client construction failed: {e}");
            return ExitCode::from(1);
        }
    };

    // One contiguous page-aligned allocation; carve N equal slices from it.
    let total = part_size * num_parts;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let mut base: *mut c_void = ptr::null_mut();
    if unsafe { libc::posix_memalign(&mut base, page, total) } != 0 {
        eprintln!("posix_memalign failed");
        return ExitCode::from(1);
    }
    unsafe { ptr::write_bytes(base as *mut u8, b'M', total) };

    let parts: Vec<RdmaPart> = (0..num_parts)
        .map(|i| {
            let ptr = unsafe { (base as *mut u8).add(i * part_size) } as *mut c_void;
            // Host buffer: compute CRC64NVME locally. GPU callers must
            // precompute on-device and supply the base64 string themselves.
            let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, part_size) };
            let checksum = crc64nvme_base64(slice);
            RdmaPart::with_checksum(unsafe { RdmaBuffer::from_raw(ptr, part_size) }, checksum)
        })
        .collect();

    println!("uploading {num_parts} parts of {part_size} bytes each ({total} total)");
    let res = client
        .rdma_put_object_multipart("my-bucket", "my-multipart-object", &parts)
        .await;

    unsafe { libc::free(base) };

    match res {
        Ok(resp) => {
            println!(
                "completed: etag={} upload_id={} bytes={} parts={}",
                resp.etag,
                resp.upload_id,
                resp.total_bytes_transferred,
                resp.parts.len()
            );
            ExitCode::from(0)
        }
        Err(e) => {
            eprintln!("multipart RDMA upload failed: {e}");
            ExitCode::from(1)
        }
    }
}
