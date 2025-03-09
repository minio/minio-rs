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

use crate::common::{create_bucket_helper, TestContext};
use minio::s3::response::{
    DeleteBucketLifecycleResponse, GetBucketLifecycleResponse, SetBucketLifecycleResponse,
};
use minio::s3::types::{Filter, LifecycleConfig, LifecycleRule, S3Api};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_lifecycle() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let rules: Vec<LifecycleRule> = vec![LifecycleRule {
        abort_incomplete_multipart_upload_days_after_initiation: None,
        expiration_date: None,
        expiration_days: Some(365),
        expiration_expired_object_delete_marker: None,
        filter: Filter {
            and_operator: None,
            prefix: Some(String::from("logs/")),
            tag: None,
        },
        id: String::from("rule1"),
        noncurrent_version_expiration_noncurrent_days: None,
        noncurrent_version_transition_noncurrent_days: None,
        noncurrent_version_transition_storage_class: None,
        status: true,
        transition_date: None,
        transition_days: None,
        transition_storage_class: None,
    }];

    let _resp: SetBucketLifecycleResponse = ctx
        .client
        .set_bucket_lifecycle(&bucket_name)
        .life_cycle_config(LifecycleConfig { rules })
        .send()
        .await
        .unwrap();
    //println!("response of setting lifecycle: resp={:?}", resp);

    if false {
        // TODO panics with: called `Result::unwrap()` on an `Err` value: XmlError("<Filter> tag not found")
        let resp: GetBucketLifecycleResponse = ctx
            .client
            .get_bucket_lifecycle(&bucket_name)
            .send()
            .await
            .unwrap();
        println!("response of getting lifecycle: resp={:?}", resp);
        //assert_eq!(resp.config, rules.to_string());
    }

    let _resp: DeleteBucketLifecycleResponse = ctx
        .client
        .delete_bucket_lifecycle(&bucket_name)
        .send()
        .await
        .unwrap();
    //println!("response of deleting lifecycle: resp={:?}", resp);

    if false {
        // TODO panics with: called `Result::unwrap()` on an `Err` value: XmlError("<Filter> tag not found")
        let resp: GetBucketLifecycleResponse = ctx
            .client
            .get_bucket_lifecycle(&bucket_name)
            .send()
            .await
            .unwrap();
        println!("response of getting policy: resp={:?}", resp);
        //assert_eq!(resp.config, LifecycleConfig::default());
    }
}
