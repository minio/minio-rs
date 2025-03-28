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
use minio::s3::builders::{DeleteBucketPolicy, GetBucketPolicy, SetBucketPolicy};
use minio::s3::types::S3Api;
use minio_common::example::create_bucket_policy_config_example;

pub(crate) fn bench_set_bucket_policy(criterion: &mut Criterion) {
    benchmark_s3_api(
        "set_bucket_policy",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            let config = create_bucket_policy_config_example(&ctx.bucket);
            SetBucketPolicy::new(&ctx.bucket)
                .client(&ctx.client)
                .config(config)
        },
    )
}
pub(crate) fn bench_get_bucket_policy(criterion: &mut Criterion) {
    benchmark_s3_api(
        "get_bucket_policy",
        criterion,
        || async {
            let ctx = Ctx2::new().await;
            let config = create_bucket_policy_config_example(&ctx.bucket);
            SetBucketPolicy::new(&ctx.bucket)
                .client(&ctx.client)
                .config(config)
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| GetBucketPolicy::new(&ctx.bucket).client(&ctx.client),
    )
}
pub(crate) fn bench_delete_bucket_policy(criterion: &mut Criterion) {
    benchmark_s3_api(
        "delete_bucket_policy",
        criterion,
        || async { Ctx2::new().await },
        |ctx| DeleteBucketPolicy::new(&ctx.bucket).client(&ctx.client),
    )
}
