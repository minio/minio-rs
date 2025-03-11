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

use crate::common::{TestContext, create_bucket_helper, rand_object_name};
use minio::s3::args::PostPolicy;
use minio::s3::utils::utc_now;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_presigned_post_form_data() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let object_name = rand_object_name();
    let expiration = utc_now() + chrono::Duration::days(5);

    let mut policy = PostPolicy::new(&bucket_name, &expiration).unwrap();
    policy.add_equals_condition("key", &object_name).unwrap();
    policy
        .add_content_length_range_condition(1024 * 1024, 4 * 1024 * 1024)
        .unwrap();

    let form_data = ctx
        .client
        .get_presigned_post_form_data(&policy)
        .await
        .unwrap();
    assert!(form_data.contains_key("x-amz-signature"));
    assert!(form_data.contains_key("policy"));
}
