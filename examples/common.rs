use minio::s3::args::{BucketExistsArgs, MakeBucketArgs};
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::{Client, ClientBuilder};

#[allow(dead_code)]
pub fn create_client_on_play() -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let base_url = "https://play.min.io".parse::<BaseUrl>()?;
    log::info!("Trying to connect to MinIO at: `{:?}`", base_url);

    let static_provider = StaticProvider::new(
        "Q3AM3UQ867SPQQA43P2F",
        "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
        None,
    );

    let client = ClientBuilder::new(base_url.clone())
        .provider(Some(Box::new(static_provider)))
        .build()?;
    Ok(client)
}

pub async fn create_bucket_if_not_exists(
    bucket_name: &str,
    client: &Client,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Check 'bucket_name' bucket exist or not.
    let exists: bool = client
        .bucket_exists(&BucketExistsArgs::new(bucket_name).unwrap())
        .await?;

    // Make 'bucket_name' bucket if not exist.
    if !exists {
        client
            .make_bucket(&MakeBucketArgs::new(bucket_name).unwrap())
            .await
            .unwrap();
    };
    Ok(())
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // dummy code just to prevent an error because files in examples need to have a main
    Ok(())
}
