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

use minio::s3::builders::PostPolicy;
use minio::s3::header_constants::*;
use minio_common::example::create_post_policy_example;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use std::collections::HashMap;

#[minio_macros::test]
async fn get_presigned_post_form_data(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();

    let policy: PostPolicy = create_post_policy_example(&bucket_name, &object_name);

    let form_data: HashMap<String, String> = ctx
        .client
        .get_presigned_post_form_data(policy)
        .build()
        .send()
        .await
        .unwrap();
    //println!("form_data={:?}", &form_data);
    assert!(form_data.contains_key(X_AMZ_SIGNATURE));
    assert!(form_data.contains_key(POLICY));
    assert!(form_data.contains_key(X_AMZ_DATE));
    assert!(form_data.contains_key(X_AMZ_ALGORITHM));
    assert!(form_data.contains_key(X_AMZ_CREDENTIAL));
}
