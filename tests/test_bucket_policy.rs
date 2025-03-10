use crate::common::{TestContext, create_bucket_helper};
use minio::s3::types::S3Api;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_policy() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let config = r#"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::<BUCKET>/myobject*"
            ],
            "Sid": ""
        }
    ]
}
"#
    .replace("<BUCKET>", &bucket_name);

    let _resp = ctx
        .client
        .set_bucket_policy(&bucket_name)
        .config(config.clone())
        .send()
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_policy(&bucket_name)
        .send()
        .await
        .unwrap();
    // TODO create a proper comparison of the retrieved config and the provided config
    // println!("response of getting policy: resp.config={:?}", resp.config);
    // assert_eq!(&resp.config, &config);
    assert!(!resp.config.is_empty());

    let _resp = ctx
        .client
        .delete_bucket_policy(&bucket_name)
        .send()
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_policy(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.config, "{}");
}
