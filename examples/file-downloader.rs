use log::info;
use minio::s3::client::ClientBuilder;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;
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

    let object_name: &str = "asiaphotos-2015.zip";

    let download_path: &str = &format!("/tmp/downloads/{object_name}");

    let get_object = client.get_object(bucket_name, object_name).send().await?;

    get_object
        .content
        .to_file(&Path::new(download_path))
        .await?;

    info!("Object `{object_name}` is successfully downloaded to file `{download_path}`.");

    Ok(())
}
