use minio::madmin::types::bucket_target::{BucketTarget, Credentials, ServiceType};

fn main() {
    let target = BucketTarget::builder()
        .source_bucket("test-bucket".to_string())
        .endpoint("http://localhost:9000".to_string())
        .target_bucket("target-bucket".to_string())
        .credentials(Some(Credentials {
            access_key: Some("minioadmin".to_string()),
            secret_key: Some("minioadmin".to_string()),
            session_token: None,
            expiration: None,
        }))
        .service_type(Some(ServiceType::Replication))
        .secure(Some(false))
        .build();

    let json = serde_json::to_string_pretty(&target).unwrap();
    println!("BucketTarget JSON:");
    println!("{}", json);
}
