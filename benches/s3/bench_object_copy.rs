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
use minio::s3::builders::{CopyObjectInternal, CopySource};
use minio_common::utils::rand_object_name;

pub(crate) fn bench_object_copy_internal(criterion: &mut Criterion) {
    benchmark_s3_api(
        "object_copy_internal",
        criterion,
        || async { Ctx2::new_with_object(false).await },
        |ctx| {
            let object_name_src = &ctx.object;
            let object_name_dst = rand_object_name();
            CopyObjectInternal::new(ctx.client.clone(), ctx.bucket.clone(), object_name_dst)
                .source(CopySource::new(&ctx.bucket, object_name_src).unwrap())
        },
    )
}
