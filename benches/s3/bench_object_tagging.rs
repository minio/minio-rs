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
use minio::s3::builders::{GetObjectTagging, PutObjectTagging};
use minio::s3::response::PutObjectTaggingResponse;
use minio::s3::types::S3Api;
use minio_common::example::create_tags_example;

pub(crate) async fn bench_put_object_tagging(criterion: &mut Criterion) {
    if skip_express_mode("bench_put_object_tagging").await {
        return;
    }
    benchmark_s3_api(
        "put_object_tagging",
        criterion,
        || async { Ctx2::new_with_object(false).await },
        |ctx| {
            PutObjectTagging::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .object(ctx.object.clone())
                .tags(create_tags_example())
                .build()
        },
    )
}
pub(crate) async fn bench_get_object_tagging(criterion: &mut Criterion) {
    if skip_express_mode("bench_get_object_tagging").await {
        return;
    }
    benchmark_s3_api(
        "get_object_tagging",
        criterion,
        || async {
            let ctx = Ctx2::new_with_object(false).await;

            let _resp: PutObjectTaggingResponse = ctx
                .client
                .put_object_tagging(&ctx.bucket, &ctx.object)
                .tags(create_tags_example())
                .build()
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| {
            GetObjectTagging::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .object(ctx.object.clone())
                .build()
        },
    )
}
