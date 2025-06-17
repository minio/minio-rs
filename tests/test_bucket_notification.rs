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
    DeleteBucketNotificationResponse, GetBucketNotificationResponse, PutBucketNotificationResponse,
};
use minio::s3::types::{NotificationConfig, S3Api};
use minio_common::example::create_bucket_notification_config_example;
use minio_common::test_context::TestContext;

const SQS_ARN: &str = "arn:minio:sqs::miniojavatest:webhook";

#[tokio::test(flavor = "multi_thread")]
async fn test_bucket_notification() {
    let ctx = TestContext::new_from_env();
    if ctx.client.is_minio_express().await {
        println!("Skipping test because it is running in MinIO Express mode");
        return;
    }

    let (bucket_name, _cleanup) = ctx.create_bucket_helper().await;

    let config: NotificationConfig = create_bucket_notification_config_example();

    let resp: PutBucketNotificationResponse = ctx
        .client
        .put_bucket_notification(&bucket_name)
        .notification_config(config.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    //println!("response of setting notification: resp={:?}", resp);

    let resp: GetBucketNotificationResponse = ctx
        .client
        .get_bucket_notification(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.config, config);
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    //println!("response of getting notification: resp={:?}", resp);

    assert_eq!(resp.config.queue_config_list.as_ref().unwrap().len(), 1);
    assert!(
        resp.config.queue_config_list.as_ref().unwrap()[0]
            .events
            .contains(&String::from("s3:ObjectCreated:Put"))
    );
    assert!(
        resp.config.queue_config_list.as_ref().unwrap()[0]
            .events
            .contains(&String::from("s3:ObjectCreated:Copy"))
    );
    assert_eq!(
        resp.config.queue_config_list.as_ref().unwrap()[0]
            .prefix_filter_rule
            .as_ref()
            .unwrap()
            .value,
        "images"
    );
    assert_eq!(
        resp.config.queue_config_list.as_ref().unwrap()[0]
            .suffix_filter_rule
            .as_ref()
            .unwrap()
            .value,
        "pg"
    );
    assert_eq!(
        resp.config.queue_config_list.as_ref().unwrap()[0].queue,
        SQS_ARN
    );

    let resp: DeleteBucketNotificationResponse = ctx
        .client
        .delete_bucket_notification(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    //println!("response of deleting notification: resp={:?}", resp);

    let resp: GetBucketNotificationResponse = ctx
        .client
        .get_bucket_notification(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket, bucket_name);
    assert_eq!(resp.region, DEFAULT_REGION);
    assert_eq!(resp.config, NotificationConfig::default());
}
