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

use crate::common_benches::{Ctx2, benchmark_s3_api};

use criterion::Criterion;
use minio::s3::builders::{DeleteBucketLifecycle, GetBucketLifecycle, PutBucketLifecycle};
use minio::s3::types::S3Api;
use minio_common::example::create_bucket_lifecycle_config_examples;

pub(crate) fn bench_put_bucket_lifecycle(criterion: &mut Criterion) {
    benchmark_s3_api(
        "put_bucket_lifecycle",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            let config = create_bucket_lifecycle_config_examples();
            PutBucketLifecycle::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .life_cycle_config(config)
                .build()
        },
    )
}
pub(crate) fn bench_get_bucket_lifecycle(criterion: &mut Criterion) {
    benchmark_s3_api(
        "get_bucket_lifecycle",
        criterion,
        || async {
            let ctx = Ctx2::new().await;
            let config = create_bucket_lifecycle_config_examples();
            ctx.client
                .put_bucket_lifecycle(&ctx.bucket)
                .life_cycle_config(config)
                .build()
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| {
            GetBucketLifecycle::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .build()
        },
    )
}
pub(crate) fn bench_delete_bucket_lifecycle(criterion: &mut Criterion) {
    benchmark_s3_api(
        "delete_bucket_lifecycle",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            DeleteBucketLifecycle::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .build()
        },
    )
}
