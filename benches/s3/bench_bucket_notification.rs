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
use minio::s3::builders::{DeleteBucketNotification, GetBucketNotification, SetBucketNotification};
use minio::s3::types::S3Api;
use minio_common::example::create_bucket_notification_config_example;

#[allow(dead_code)]
pub(crate) fn bench_set_bucket_notification(criterion: &mut Criterion) {
    benchmark_s3_api(
        "set_bucket_notification",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            let config = create_bucket_notification_config_example();
            SetBucketNotification::new(ctx.client.clone(), ctx.bucket.clone())
                .notification_config(config)
        },
    )
}
#[allow(dead_code)]
pub(crate) fn bench_get_bucket_notification(criterion: &mut Criterion) {
    benchmark_s3_api(
        "get_bucket_notification",
        criterion,
        || async {
            let ctx = Ctx2::new().await;
            let config = create_bucket_notification_config_example();
            ctx.client
                .set_bucket_notification(&ctx.bucket)
                .notification_config(config)
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| GetBucketNotification::new(ctx.client.clone(), ctx.bucket.clone()),
    )
}
#[allow(dead_code)]
pub(crate) fn bench_delete_bucket_notification(criterion: &mut Criterion) {
    benchmark_s3_api(
        "delete_bucket_notification",
        criterion,
        || async { Ctx2::new().await },
        |ctx| DeleteBucketNotification::new(ctx.client.clone(), ctx.bucket.clone()),
    )
}
