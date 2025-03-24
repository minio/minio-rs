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

use criterion::{Criterion, criterion_group, criterion_main};
use minio::s3::utils::check_bucket_name;

fn bench_check_bucket_name(c: &mut Criterion) {
    c.bench_function("check_bucket_name true", |b| {
        b.iter(|| check_bucket_name("my-example-bucket-name", true))
    });

    c.bench_function("check_bucket_name false", |b| {
        b.iter(|| check_bucket_name("my-example-bucket-name", false))
    });
}

criterion_group!(benches, bench_check_bucket_name);
criterion_main!(benches);
