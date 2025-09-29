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
use std::sync::Arc;

use criterion::Criterion;
use minio::s3::builders::AppendObject;
use minio::s3::error::Error;
use minio::s3::response::StatObjectResponse;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;
use tokio::task;

#[allow(dead_code)]
pub(crate) async fn bench_object_append(criterion: &mut Criterion) {
    if !TestContext::new_from_env().client.is_minio_express().await {
        println!("Skipping benchmark because it is NOT running in MinIO Express mode");
        return;
    }
    benchmark_s3_api(
        "object_append",
        criterion,
        || async { Ctx2::new_with_object(false).await },
        |ctx| {
            let content1 = "Hello world 2";
            let data1: SegmentedBytes = SegmentedBytes::from(content1.to_string());

            let resp: StatObjectResponse = task::block_in_place(|| {
                let runtime =
                    tokio::runtime::Runtime::new().map_err(|e| Error::DriveIo(e.into()))?;
                runtime.block_on(
                    ctx.client
                        .stat_object(&ctx.bucket, &ctx.object)
                        .build()
                        .send(),
                )
            })
            .unwrap();

            let offset_bytes: u64 = resp.size().unwrap();
            AppendObject::builder()
                .client(ctx.client.clone())
                .bucket(ctx.bucket.clone())
                .object(ctx.object.clone())
                .data(Arc::new(data1))
                .offset_bytes(offset_bytes)
                .build()
        },
    )
}
