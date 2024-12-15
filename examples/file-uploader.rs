use log::info;
use minio::s3::args::{BucketExistsArgs, MakeBucketArgs};
use minio::s3::builders::ObjectContent;
use minio::s3::client::ClientBuilder;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init(); // Note: set environment variable RUST_LOG="INFO" to log info and higher

    let base_url = "https://play.min.io".parse::<BaseUrl>()?;

    info!("Trying to connect to MinIO at: `{:?}`", base_url);

    let static_provider = StaticProvider::new(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
        None,
    );

    let client = ClientBuilder::new(base_url.clone())
        .provider(Some(Box::new(static_provider)))
        .build()?;

    let bucket_name: &str = "asiatrip";

    // Check 'bucket_name' bucket exist or not.
    let exists: bool = client
        .bucket_exists(&BucketExistsArgs::new(bucket_name).unwrap())
        .await
        .unwrap();

    // Make 'bucket_name' bucket if not exist.
    if !exists {
        client
            .make_bucket(&MakeBucketArgs::new(bucket_name).unwrap())
            .await
            .unwrap();
    }

    // File we are going to upload to the bucket
    let filename: &Path = Path::new("/tmp/asiaphotos.zip");

    // Name of the object that will be stored in the bucket
    let object_name: &str = "asiaphotos-2015.zip";

    info!("filename {}", &filename.to_str().unwrap());

    let content = ObjectContent::from(filename);
    client
        .put_object_content(bucket_name, object_name, content)
        .send()
        .await?;

    info!(
        "file `{}` is successfully uploaded as object `{object_name}` to bucket `{bucket_name}`.",
        filename.display()
    );
    Ok(())
}
