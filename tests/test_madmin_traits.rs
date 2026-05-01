// Test to verify madmin response traits work correctly

use bytes::Bytes;
use http::HeaderMap;
use minio::impl_has_madmin_fields;
use minio::madmin::madmin_client::MadminClient;
use minio::madmin::response::response_traits::{HasBucket, HasMadminFields};
use minio::madmin::types::MadminRequest;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;

#[test]
fn test_madmin_traits_compile() {
    // This test just verifies that the trait system compiles correctly

    #[derive(Clone, Debug)]
    struct TestResponse {
        request: MadminRequest,
        headers: HeaderMap,
        body: Bytes,
        #[allow(dead_code)]
        data: String,
    }

    impl_has_madmin_fields!(TestResponse);
    impl HasBucket for TestResponse {}

    // Create a mock response
    let base_url = "http://localhost:9000".parse::<BaseUrl>().unwrap();
    let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MadminClient::new(base_url, Some(provider));

    let request = MadminRequest::builder()
        .client(client)
        .method(http::Method::GET)
        .path("/test")
        .bucket(Some("my-bucket".to_string()))
        .build();

    let response = TestResponse {
        request,
        headers: HeaderMap::new(),
        body: Bytes::new(),
        data: "test".to_string(),
    };

    // Test HasMadminFields trait
    assert_eq!(response.request().get_bucket(), Some("my-bucket"));
    assert_eq!(response.headers().len(), 0);
    assert_eq!(response.body().len(), 0);

    // Test HasBucket trait
    assert_eq!(response.bucket(), Some("my-bucket"));

    println!("✓ Madmin traits work correctly!");
}
