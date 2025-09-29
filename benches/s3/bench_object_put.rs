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
use minio::s3::builders::{ObjectContent, PutObject, UploadPart};
use minio::s3::segmented_bytes::SegmentedBytes;
use minio_common::rand_src::RandSrc;
use minio_common::utils::rand_object_name;
use tokio::task;

pub(crate) fn bench_object_put(criterion: &mut Criterion) {
    benchmark_s3_api(
        "object_put",
        criterion,
        || async { Ctx2::new().await },
        |ctx| {
            let object_name: String = rand_object_name();
            let size = 1024 * 1024_u64; // 1MB
            let object_content = ObjectContent::new_from_stream(RandSrc::new(size), Some(size));

            let data: SegmentedBytes = task::block_in_place(|| {
                tokio::runtime::Runtime::new()?.block_on(object_content.to_segmented_bytes())
            })
            .unwrap();

            PutObject::builder()
                .inner(
                    UploadPart::builder()
                        .client(ctx.client.clone())
                        .bucket(ctx.bucket.clone())
                        .object(object_name)
                        .data(Arc::new(data))
                        .build(),
                )
                .build()
        },
    )
}
