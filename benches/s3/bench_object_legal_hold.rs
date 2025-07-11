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
use minio::s3::builders::{GetObjectLegalHold, PutObjectLegalHold};
use minio::s3::types::S3Api;

pub(crate) async fn bench_put_object_legal_hold(criterion: &mut Criterion) {
    if skip_express_mode("bench_put_object_legal_hold").await {
        return;
    }
    benchmark_s3_api(
        "put_object_legal_hold",
        criterion,
        || async { Ctx2::new_with_object(true).await },
        |ctx| {
            PutObjectLegalHold::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .object(ctx.object.clone())
                .legal_hold(true)
                .build()
        },
    )
}
pub(crate) async fn bench_get_object_legal_hold(criterion: &mut Criterion) {
    if skip_express_mode("bench_get_object_legal_hold").await {
        return;
    }
    benchmark_s3_api(
        "get_object_legal_hold",
        criterion,
        || async {
            let ctx = Ctx2::new_with_object(true).await;
            ctx.client
                .get_object_legal_hold(&ctx.bucket, &ctx.object)
                .build()
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| {
            GetObjectLegalHold::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .object(ctx.object.clone())
                .build()
        },
    )
}
