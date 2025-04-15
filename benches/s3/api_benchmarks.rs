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

mod bench_bucket_exists;
mod bench_bucket_lifecycle;
mod bench_bucket_notification;
mod bench_bucket_policy;
mod bench_bucket_replication;
mod bench_bucket_tags;
mod bench_bucket_versioning;
mod bench_list_bucket;
mod bench_object_append;
mod bench_object_copy;
mod bench_object_legal_hold;
mod bench_object_lock_config;
mod bench_object_put;
mod bench_object_retention;
mod bench_object_tags;
mod common_benches;

use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

use crate::bench_bucket_exists::*;
use crate::bench_bucket_lifecycle::*;
#[allow(unused_imports)]
use crate::bench_bucket_notification::*;
use crate::bench_bucket_policy::*;
#[allow(unused_imports)]
use crate::bench_bucket_replication::*;
use crate::bench_bucket_tags::*;
use crate::bench_bucket_versioning::*;
use crate::bench_list_bucket::*;
#[allow(unused_imports)]
use crate::bench_object_append::bench_object_append;
#[allow(unused_imports)]
use crate::bench_object_copy::*;
use crate::bench_object_legal_hold::*;
use crate::bench_object_lock_config::*;
use crate::bench_object_put::bench_object_put;
use crate::bench_object_retention::*;
use crate::bench_object_tags::*;

criterion_group!(
    name = benches;
    config = Criterion::default()
        .configure_from_args()
        .warm_up_time(Duration::from_secs_f32(0.01))
        .sample_size(1000)
        .nresamples(1001)
        .measurement_time(Duration::from_secs_f32(10.0));
    targets =
        bench_bucket_exists,
        bench_set_bucket_lifecycle,
        bench_get_bucket_lifecycle,
        bench_delete_bucket_lifecycle,
        //
        //bench_set_bucket_notification, //A specified destination ARN does not exist or is not well-formed
        //bench_get_bucket_notification,
        //bench_delete_bucket_notification,
        //
        bench_set_bucket_policy,
        bench_get_bucket_policy,
        bench_delete_bucket_policy,
        //
        //bench_set_bucket_replication, //TODO setup permissions to allow replication
        //bench_get_bucket_replication,
        //bench_delete_bucket_replication,
        //
        bench_set_bucket_tags,
        bench_get_bucket_tags,
        bench_delete_bucket_tags,
        //
        bench_set_bucket_versioning,
        bench_get_bucket_versioning,
        //
        bench_list_buckets,
        bench_object_copy_internal,
        //bench_object_append, // TODO: add support to switch on/off s3-express
        bench_object_put,
        //
        bench_enable_object_legal_hold,
        bench_disable_object_legal_hold,
        bench_is_object_legal_hold,
        //
        bench_set_object_lock_config,
        bench_get_object_lock_config,
        bench_delete_object_lock_config,
        //
        bench_set_object_retention,
        bench_get_object_retention,
        //
        bench_set_object_tags,
        bench_get_object_tags
);

criterion_main!(benches);
