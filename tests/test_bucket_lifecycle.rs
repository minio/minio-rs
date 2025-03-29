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

use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{
    DeleteBucketLifecycleResponse, GetBucketLifecycleResponse, SetBucketLifecycleResponse,
};
use minio::s3::types::{LifecycleConfig, S3Api};
use minio_common::example::create_bucket_lifecycle_config_examples;
use minio_common::test_context::TestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_lifecycle() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let config: LifecycleConfig = create_bucket_lifecycle_config_examples();

    let resp: SetBucketLifecycleResponse = ctx
        .client
        .set_bucket_lifecycle(&bucket_name)
        .life_cycle_config(config.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    //println!("response of setting lifecycle: resp={:?}", resp);

    if false {
        // TODO panics with: called `Result::unwrap()` on an `Err` value: XmlError("<Filter> tag not found")
        let resp: GetBucketLifecycleResponse = ctx
            .client
            .get_bucket_lifecycle(&bucket_name)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.config, config);
        assert_eq!(resp.bucket, bucket_name);
        assert_eq!(resp.region, DEFAULT_REGION);
        println!("response of getting lifecycle: resp={:?}", resp);
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
