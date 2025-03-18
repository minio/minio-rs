mod common;

use crate::common::{TestContext, create_bucket_helper};
use minio::s3::client::DEFAULT_REGION;
use minio::s3::response::{
    DeleteBucketNotificationResponse, GetBucketNotificationResponse, SetBucketNotificationResponse,
};
use minio::s3::types::{
    NotificationConfig, PrefixFilterRule, QueueConfig, S3Api, SuffixFilterRule,
};

const SQS_ARN: &str = "arn:minio:sqs::miniojavatest:webhook";

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_notification() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let config = NotificationConfig {
        cloud_func_config_list: None,
        queue_config_list: Some(vec![QueueConfig {
            events: vec![
                String::from("s3:ObjectCreated:Put"),
                String::from("s3:ObjectCreated:Copy"),
            ],
            id: Some("".to_string()), //TODO or should this be NONE??
            prefix_filter_rule: Some(PrefixFilterRule {
                value: String::from("images"),
            }),
            suffix_filter_rule: Some(SuffixFilterRule {
                value: String::from("pg"),
            }),
            queue: String::from(SQS_ARN),
        }]),
        topic_config_list: None,
    };

    let resp: SetBucketNotificationResponse = ctx
        .client
        .set_bucket_notification(&bucket_name)
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
