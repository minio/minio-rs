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
use minio::s3::builders::{GetObjectRetention, SetObjectRetention};
use minio::s3::response::SetObjectRetentionResponse;
use minio::s3::types::{RetentionMode, S3Api};
use minio::s3::utils::utc_now;

pub(crate) fn bench_set_object_retention(criterion: &mut Criterion) {
    benchmark_s3_api(
        "set_object_retention",
        criterion,
        || async { Ctx2::new_with_object(true).await },
        |ctx| {
            SetObjectRetention::new(&ctx.client, ctx.bucket.to_owned(), ctx.object.to_owned())
                .retention_mode(Some(RetentionMode::GOVERNANCE))
                .retain_until_date(Some(utc_now() + chrono::Duration::days(1)))
        },
    )
}
pub(crate) fn bench_get_object_retention(criterion: &mut Criterion) {
    benchmark_s3_api(
        "get_object_retention",
        criterion,
        || async {
            let ctx = Ctx2::new_with_object(true).await;
            let _resp: SetObjectRetentionResponse =
                SetObjectRetention::new(&ctx.client, ctx.bucket.to_owned(), ctx.object.to_owned())
                    .retention_mode(Some(RetentionMode::GOVERNANCE))
                    .retain_until_date(Some(utc_now() + chrono::Duration::days(1)))
                    .send()
                    .await
                    .unwrap();
            ctx
        },
        |ctx| GetObjectRetention::new(&ctx.client, ctx.bucket.to_owned(), ctx.object.to_owned()),
    )
}
