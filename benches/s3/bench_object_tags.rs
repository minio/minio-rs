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
use minio::s3::builders::{GetObjectTags, SetObjectTags};
use minio::s3::response::SetObjectTagsResponse;
use minio::s3::types::S3Api;
use minio_common::example::create_tags_example;

pub(crate) fn bench_set_object_tags(criterion: &mut Criterion) {
    benchmark_s3_api(
        "set_object_tags",
        criterion,
        || async { Ctx2::new_with_object(false).await },
        |ctx| {
            SetObjectTags::new(&ctx.client, ctx.bucket.to_owned(), ctx.object.to_owned())
                .tags(create_tags_example())
        },
    )
}
pub(crate) fn bench_get_object_tags(criterion: &mut Criterion) {
    benchmark_s3_api(
        "get_object_tags",
        criterion,
        || async {
            let ctx = Ctx2::new_with_object(false).await;

            let _resp: SetObjectTagsResponse = ctx
                .client
                .set_object_tags(&ctx.bucket, &ctx.object)
                .tags(create_tags_example())
                .send()
                .await
                .unwrap();
            ctx
        },
        |ctx| GetObjectTags::new(&ctx.client, ctx.bucket.to_owned(), ctx.object.to_owned()),
    )
}
