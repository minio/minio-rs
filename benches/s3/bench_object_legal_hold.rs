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
use minio::s3::builders::{
    DisableObjectLegalHold, EnableObjectLegalHold, IsObjectLegalHoldEnabled,
};
use minio::s3::types::S3Api;

pub(crate) fn bench_enable_object_legal_hold(criterion: &mut Criterion) {
    if skip_express_mode("bench_enable_object_legal_hold") {
        return;
    }
    benchmark_s3_api(
        "enable_object_legal_hold",
        criterion,
        || async { Ctx2::new_with_object(true).await },
        |ctx| {
            EnableObjectLegalHold::new(ctx.client.clone(), ctx.bucket.clone(), ctx.object.clone())
        },
    )
}
pub(crate) fn bench_disable_object_legal_hold(criterion: &mut Criterion) {
    if skip_express_mode("bench_disable_object_legal_hold") {
        return;
    }
    benchmark_s3_api(
        "disable_object_legal_hold",
        criterion,
        || async { Ctx2::new_with_object(true).await },
        |ctx| {
            DisableObjectLegalHold::new(ctx.client.clone(), ctx.bucket.clone(), ctx.object.clone())
        },
    )
}
pub(crate) fn bench_is_object_legal_hold(criterion: &mut Criterion) {
    if skip_express_mode("bench_is_object_legal_hold") {
        return;
    }
    benchmark_s3_api(
        "is_object_legal_hold",
        criterion,
        || async {
            let ctx = Ctx2::new_with_object(true).await;
            ctx.client
                .enable_object_legal_hold(&ctx.bucket, &ctx.object)
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| {
            IsObjectLegalHoldEnabled::new(
                ctx.client.clone(),
                ctx.bucket.clone(),
                ctx.object.clone(),
            )
        },
    )
}
