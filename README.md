# MinIO Rust SDK for Amazon S3 Compatible Cloud Storage [![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) [![Sourcegraph](https://sourcegraph.com/github.com/minio/minio-rs/-/badge.svg)](https://sourcegraph.com/github.com/minio/minio-rs?badge) [![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/minio/minio-rs/blob/master/LICENSE)

MinIO Rust SDK is Simple Storage Service (aka S3) client to perform bucket and object operations to any Amazon S3 compatible object storage service.

For a complete list of APIs and examples, please take a look at the [MinIO Rust Client API Reference](https://minio-rs.min.io/)

## Example:: file-uploader.rs
```rust
use minio::s3::args::{BucketExistsArgs, MakeBucketArgs, UploadObjectArgs};
use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut base_url = BaseUrl::from_string("play.min.io").unwrap();
    base_url.https = true;

    let static_provider = StaticProvider::new(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
        None,
    );

    let mut client = Client::new(base_url.clone(), Some(&static_provider), None, None).unwrap();

    let bucket_name = "asiatrip";

    // Check 'asiatrip' bucket exist or not.
    let exists = client
        .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    // Make 'asiatrip' bucket if not exist.
    if !exist {
        client
            .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
            .await
            .unwrap();
    }

    // Upload '/home/user/Photos/asiaphotos.zip' as object name
    // 'asiaphotos-2015.zip' to bucket 'asiatrip'.
    client
        .upload_object(
            &mut UploadObjectArgs::new(
                &bucket_name,
                "asiaphotos-2015.zip",
                "/home/user/Photos/asiaphotos.zip",
            )
            .unwrap(),
        )
        .await
        .unwrap();

    println!("'/home/user/Photos/asiaphotos.zip' is successfully uploaded as object 'asiaphotos-2015.zip' to bucket 'asiatrip'.");
}
```

## License
This SDK is distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0), see [LICENSE](https://github.com/minio/minio-rs/blob/master/LICENSE) for more information.
