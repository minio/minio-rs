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

use crate::common_benches::{Ctx2, benchmark_s3_api, skip_express_mode};

use criterion::Criterion;
use minio::s3::builders::{GetBucketVersioning, PutBucketVersioning, VersioningStatus};

pub(crate) async fn bench_get_bucket_versioning(criterion: &mut Criterion) {
    if skip_express_mode("bench_get_bucket_versioning").await {
        return;
    }
    benchmark_s3_api(
        "get_bucket_versioning",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            GetBucketVersioning::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .build()
        },
    )
}
pub(crate) async fn bench_put_bucket_versioning(criterion: &mut Criterion) {
    if skip_express_mode("bench_put_bucket_versioning").await {
        return;
    }
    benchmark_s3_api(
        "put_bucket_versioning",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            PutBucketVersioning::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .versioning_status(VersioningStatus::Enabled)
                .build()
        },
    )
}
