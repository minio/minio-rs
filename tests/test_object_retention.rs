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

mod common;

use crate::common::{CleanupGuard, TestContext, rand_bucket_name, rand_object_name};
use common::RandSrc;
use minio::s3::args::{GetObjectRetentionArgs, MakeBucketArgs, SetObjectRetentionArgs};
use minio::s3::builders::ObjectContent;
use minio::s3::types::{RetentionMode, S3Api};
use minio::s3::utils::{to_iso8601utc, utc_now};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn object_retention() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    let mut args = MakeBucketArgs::new(&bucket_name).unwrap();
    args.object_lock = true;
    ctx.client.make_bucket(&args).await.unwrap();
    let _cleanup = CleanupGuard::new(&ctx, &bucket_name);

    let object_name = rand_object_name();

    let size = 16_u64;

    let obj_resp = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .send()
        .await
        .unwrap();

    let retain_until_date = utc_now() + chrono::Duration::days(1);
    let args = SetObjectRetentionArgs::new(
        &bucket_name,
        &object_name,
        Some(RetentionMode::GOVERNANCE),
        Some(retain_until_date),
    )
    .unwrap();

    ctx.client.set_object_retention(&args).await.unwrap();

    let resp = ctx
        .client
        .get_object_retention(&GetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(match resp.retention_mode {
        Some(v) => matches!(v, RetentionMode::GOVERNANCE),
        _ => false,
    });
    assert!(match resp.retain_until_date {
        Some(v) => to_iso8601utc(v) == to_iso8601utc(retain_until_date),
        _ => false,
    },);

    let mut args = SetObjectRetentionArgs::new(&bucket_name, &object_name, None, None).unwrap();
    args.bypass_governance_mode = true;
    ctx.client.set_object_retention(&args).await.unwrap();

    let resp = ctx
        .client
        .get_object_retention(&GetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(resp.retention_mode.is_none());
    assert!(resp.retain_until_date.is_none());

    ctx.client
        .remove_object(
            &bucket_name,
            (object_name.as_str(), obj_resp.version_id.as_deref()),
        )
        .send()
        .await
        .unwrap();
}
