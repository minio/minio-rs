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

#[allow(unused_imports)]
use crate::common_benches::{Ctx2, benchmark_s3_api};

use criterion::Criterion;

#[allow(dead_code)]
pub(crate) fn bench_object_copy(_criterion: &mut Criterion) {
    /*
    benchmark_s3_api(
        "object_copy",
        criterion,
        || async { Ctx2::new_with_object(false).await },
        |ctx| {
            let _object_name_dst = rand_object_name();
            //TODO refactor copy object for this to be possible
            todo!()
        },
    );
     */
}
