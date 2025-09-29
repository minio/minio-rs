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
use minio::s3::builders::{
    DeleteBucketReplication, GetBucketReplication, PutBucketReplication, VersioningStatus,
};
use minio::s3::response::PutBucketVersioningResponse;
use minio::s3::types::S3Api;
use minio_common::example::create_bucket_replication_config_example;

#[allow(dead_code)]
pub(crate) fn bench_put_bucket_replication(criterion: &mut Criterion) {
    benchmark_s3_api(
        "put_bucket_replication",
        criterion,
        || async {
            let mut ctx = Ctx2::new().await;
            ctx.new_aux().await;

            let _resp: PutBucketVersioningResponse = ctx
                .client
                .put_bucket_versioning(&ctx.bucket)
                .versioning_status(VersioningStatus::Enabled)
                .build()
                .send()
                .await
                .unwrap();

            let _resp: PutBucketVersioningResponse = ctx
                .client
                .put_bucket_versioning(ctx.aux_bucket.clone().unwrap())
                .versioning_status(VersioningStatus::Enabled)
                .build()
                .send()
                .await
                .unwrap();

            ctx
        },
        |ctx| {
            let config =
                create_bucket_replication_config_example(ctx.aux_bucket.clone().unwrap().as_str());
            PutBucketReplication::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .replication_config(config)
                .build()
        },
    )
}
#[allow(dead_code)]
pub(crate) fn bench_get_bucket_replication(criterion: &mut Criterion) {
    benchmark_s3_api(
        "get_bucket_replication",
        criterion,
        || async {
            let mut ctx = Ctx2::new().await;
            ctx.new_aux().await;

            let _resp: PutBucketVersioningResponse = ctx
                .client
                .put_bucket_versioning(&ctx.bucket)
                .versioning_status(VersioningStatus::Enabled)
                .build()
                .send()
                .await
                .unwrap();

            let _resp: PutBucketVersioningResponse = ctx
                .client
                .put_bucket_versioning(ctx.aux_bucket.clone().unwrap())
                .versioning_status(VersioningStatus::Enabled)
                .build()
                .send()
                .await
                .unwrap();

            ctx
        },
        |ctx| {
            GetBucketReplication::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .build()
        },
    )
}
#[allow(dead_code)]
pub(crate) fn bench_delete_bucket_replication(criterion: &mut Criterion) {
    benchmark_s3_api(
        "delete_bucket_replication",
        criterion,
        || async {
            let mut ctx = Ctx2::new().await;
            ctx.new_aux().await;

            let _resp: PutBucketVersioningResponse = ctx
                .client
                .put_bucket_versioning(&ctx.bucket)
                .versioning_status(VersioningStatus::Enabled)
                .build()
                .send()
                .await
                .unwrap();

            let _resp: PutBucketVersioningResponse = ctx
                .client
                .put_bucket_versioning(ctx.aux_bucket.clone().unwrap())
                .versioning_status(VersioningStatus::Enabled)
                .build()
                .send()
                .await
                .unwrap();

            ctx
        },
        |ctx| {
            DeleteBucketReplication::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .build()
        },
    )
}
