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

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use minio::s3::utils::{
    crc32_checksum, crc32c, crc64nvme_checksum, md5sum_hash, sha1_hash, sha256_checksum,
};

fn bench_checksums(c: &mut Criterion) {
    let sizes = vec![
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
        ("1MB", 1024 * 1024),
        ("10MB", 10 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        let data = vec![0u8; size];

        let mut group = c.benchmark_group(format!("checksum_{}", name));
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function("CRC32", |b| b.iter(|| crc32_checksum(&data)));

        group.bench_function("CRC32C", |b| b.iter(|| crc32c(&data)));

        group.bench_function("CRC64NVME", |b| b.iter(|| crc64nvme_checksum(&data)));

        group.bench_function("MD5", |b| b.iter(|| md5sum_hash(&data)));

        group.bench_function("SHA1", |b| b.iter(|| sha1_hash(&data)));

        group.bench_function("SHA256", |b| b.iter(|| sha256_checksum(&data)));

        group.finish();
    }
}

criterion_group!(benches, bench_checksums);
criterion_main!(benches);
