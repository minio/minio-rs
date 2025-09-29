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
use minio::s3::response::a_response_traits::{HasBucket, HasRegion};
use minio::s3::response::{
    DeleteBucketNotificationResponse, GetBucketNotificationResponse, PutBucketNotificationResponse,
};
use minio::s3::types::{NotificationConfig, S3Api};
use minio_common::example::create_bucket_notification_config_example;
use minio_common::test_context::TestContext;

const SQS_ARN: &str = "arn:minio:sqs::miniojavatest:webhook";

#[minio_macros::test(skip_if_express)]
async fn test_bucket_notification(ctx: TestContext, bucket_name: String) {
    let config: NotificationConfig = create_bucket_notification_config_example();

    let resp: PutBucketNotificationResponse = ctx
        .client
        .put_bucket_notification(&bucket_name)
        .notification_config(config.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    //println!("response of setting notification: resp={:?}", resp);

    let resp: GetBucketNotificationResponse = ctx
        .client
        .get_bucket_notification(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    let config2 = resp.config().unwrap();
    assert_eq!(config2, config);
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    //println!("response of getting notification: resp={:?}", resp);

    assert_eq!(config2.queue_config_list.as_ref().unwrap().len(), 1);
    assert!(
        config2.queue_config_list.as_ref().unwrap()[0]
            .events
            .contains(&String::from("s3:ObjectCreated:Put"))
    );
    assert!(
        config2.queue_config_list.as_ref().unwrap()[0]
            .events
            .contains(&String::from("s3:ObjectCreated:Copy"))
    );
    assert_eq!(
        config2.queue_config_list.as_ref().unwrap()[0]
            .prefix_filter_rule
            .as_ref()
            .unwrap()
            .value,
        "images"
    );
    assert_eq!(
        config2.queue_config_list.as_ref().unwrap()[0]
            .suffix_filter_rule
            .as_ref()
            .unwrap()
            .value,
        "pg"
    );
    assert_eq!(
        config2.queue_config_list.as_ref().unwrap()[0].queue,
        SQS_ARN
    );

    let resp: DeleteBucketNotificationResponse = ctx
        .client
        .delete_bucket_notification(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    //println!("response of deleting notification: resp={:?}", resp);

    let resp: GetBucketNotificationResponse = ctx
        .client
        .get_bucket_notification(&bucket_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.region(), DEFAULT_REGION);
    assert_eq!(resp.config().unwrap(), NotificationConfig::default());
}
