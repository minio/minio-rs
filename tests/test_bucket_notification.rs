mod common;

use crate::common::{create_bucket_helper, TestContext};
use minio::s3::args::{
    DeleteBucketNotificationArgs, GetBucketNotificationArgs, SetBucketNotificationArgs,
};
use minio::s3::types::{NotificationConfig, PrefixFilterRule, QueueConfig, SuffixFilterRule};

const SQS_ARN: &str = "arn:minio:sqs::miniojavatest:webhook";

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_notification() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    ctx.client
        .set_bucket_notification(
            &SetBucketNotificationArgs::new(
                &bucket_name,
                &NotificationConfig {
                    cloud_func_config_list: None,
                    queue_config_list: Some(vec![QueueConfig {
                        events: vec![
                            String::from("s3:ObjectCreated:Put"),
                            String::from("s3:ObjectCreated:Copy"),
                        ],
                        id: None,
                        prefix_filter_rule: Some(PrefixFilterRule {
                            value: String::from("images"),
                        }),
                        suffix_filter_rule: Some(SuffixFilterRule {
                            value: String::from("pg"),
                        }),
                        queue: String::from(SQS_ARN),
                    }]),
                    topic_config_list: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_notification(&GetBucketNotificationArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.config.queue_config_list.as_ref().unwrap().len(), 1);
    assert!(resp.config.queue_config_list.as_ref().unwrap()[0]
        .events
        .contains(&String::from("s3:ObjectCreated:Put")));
    assert!(resp.config.queue_config_list.as_ref().unwrap()[0]
        .events
        .contains(&String::from("s3:ObjectCreated:Copy")));
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

    ctx.client
        .delete_bucket_notification(&DeleteBucketNotificationArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_notification(&GetBucketNotificationArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.config.queue_config_list.is_none());
}
