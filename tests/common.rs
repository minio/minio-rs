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

use async_std::task;
use bytes::Bytes;
use rand::SeedableRng;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::SmallRng;
use std::path::{Path, PathBuf};
use std::{io, thread};
use tokio::io::AsyncRead;
use tokio::time::timeout;
use tokio_stream::Stream;

use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;

pub struct RandReader {
    size: u64,
}

impl RandReader {
    #[allow(dead_code)]
    pub fn new(size: u64) -> RandReader {
        RandReader { size }
    }
}

impl io::Read for RandReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let bytes_read: usize = match (self.size as usize) > buf.len() {
            true => buf.len(),
            false => self.size as usize,
        };

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut rand::thread_rng();
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        self.size -= bytes_read as u64;

        Ok(bytes_read)
    }
}

pub struct RandSrc {
    size: u64,
    rng: SmallRng,
}

impl RandSrc {
    #[allow(dead_code)]
    pub fn new(size: u64) -> RandSrc {
        let rng = SmallRng::from_entropy();
        RandSrc { size, rng }
    }
}

impl Stream for RandSrc {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        if self.size == 0 {
            return task::Poll::Ready(None);
        }

        let bytes_read = match self.size > 64 * 1024 {
            true => 64 * 1024,
            false => self.size as usize,
        };

        let this = self.get_mut();

        let mut buf = vec![0; bytes_read];
        let random: &mut dyn rand::RngCore = &mut this.rng;
        random.fill_bytes(&mut buf);

        this.size -= bytes_read as u64;

        task::Poll::Ready(Some(Ok(Bytes::from(buf))))
    }
}

impl AsyncRead for RandSrc {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
        read_buf: &mut tokio::io::ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        let buf = read_buf.initialize_unfilled();
        let bytes_read = match self.size > (buf.len() as u64) {
            true => buf.len(),
            false => self.size as usize,
        };

        let this = self.get_mut();

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut this.rng;
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        this.size -= bytes_read as u64;

        read_buf.advance(bytes_read);
        task::Poll::Ready(Ok(()))
    }
}

pub fn rand_bucket_name() -> String {
    Alphanumeric
        .sample_string(&mut rand::thread_rng(), 8)
        .to_lowercase()
}

#[allow(dead_code)]
pub fn rand_object_name() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), 8)
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct TestContext {
    pub base_url: BaseUrl,
    pub access_key: String,
    pub secret_key: String,
    pub ignore_cert_check: Option<bool>,
    pub ssl_cert_file: Option<PathBuf>,
    pub client: Client,
}

impl TestContext {
    #[allow(dead_code)]
    pub fn new_from_env() -> Self {
        let run_on_ci: bool = std::env::var("CI")
            .unwrap_or("false".into())
            .parse()
            .unwrap_or(false);
        if run_on_ci {
            let host = std::env::var("SERVER_ENDPOINT").unwrap();
            let access_key = std::env::var("ACCESS_KEY").unwrap();
            let secret_key = std::env::var("SECRET_KEY").unwrap();
            let secure = std::env::var("ENABLE_HTTPS").is_ok();
            let value = std::env::var("SSL_CERT_FILE").unwrap();
            let mut ssl_cert_file = None;
            if !value.is_empty() {
                ssl_cert_file = Some(Path::new(&value));
            }
            let ignore_cert_check = std::env::var("IGNORE_CERT_CHECK").is_ok();
            let region = std::env::var("SERVER_REGION").ok();

            let mut base_url: BaseUrl = host.parse().unwrap();
            base_url.https = secure;
            if let Some(v) = region {
                base_url.region = v;
            }

            let static_provider = StaticProvider::new(&access_key, &secret_key, None);
            let client = Client::new(
                base_url.clone(),
                Some(Box::new(static_provider)),
                ssl_cert_file,
                Some(ignore_cert_check),
            )
            .unwrap();

            Self {
                base_url,
                access_key,
                secret_key,
                ignore_cert_check: Some(ignore_cert_check),
                ssl_cert_file: ssl_cert_file.map(PathBuf::from),
                client,
            }
        } else {
            const DEFAULT_SERVER_ENDPOINT: &str = "https://play.min.io/";
            const DEFAULT_ACCESS_KEY: &str = "minioadmin";
            const DEFAULT_SECRET_KEY: &str = "minioadmin";
            const DEFAULT_ENABLE_HTTPS: &str = "true";
            const DEFAULT_SSL_CERT_FILE: &str = "./tests/public.crt";
            const DEFAULT_IGNORE_CERT_CHECK: &str = "false";
            const DEFAULT_SERVER_REGION: &str = "";

            let host: String =
                std::env::var("SERVER_ENDPOINT").unwrap_or(DEFAULT_SERVER_ENDPOINT.to_string());
            log::info!("SERVER_ENDPOINT={}", host);
            let access_key: String =
                std::env::var("ACCESS_KEY").unwrap_or(DEFAULT_ACCESS_KEY.to_string());
            log::info!("ACCESS_KEY={}", access_key);
            let secret_key: String =
                std::env::var("SECRET_KEY").unwrap_or(DEFAULT_SECRET_KEY.to_string());
            log::info!("SECRET_KEY=*****");
            let secure: bool = std::env::var("ENABLE_HTTPS")
                .unwrap_or(DEFAULT_ENABLE_HTTPS.to_string())
                .parse()
                .unwrap_or(false);
            log::info!("ENABLE_HTTPS={}", secure);
            let ssl_cert: String =
                std::env::var("SSL_CERT_FILE").unwrap_or(DEFAULT_SSL_CERT_FILE.to_string());
            log::info!("SSL_CERT_FILE={}", ssl_cert);
            let ssl_cert_file: PathBuf = ssl_cert.into();
            let ignore_cert_check: bool = std::env::var("IGNORE_CERT_CHECK")
                .unwrap_or(DEFAULT_IGNORE_CERT_CHECK.to_string())
                .parse()
                .unwrap_or(true);
            log::info!("IGNORE_CERT_CHECK={}", ignore_cert_check);
            let region: String =
                std::env::var("SERVER_REGION").unwrap_or(DEFAULT_SERVER_REGION.to_string());
            log::info!("SERVER_REGION={:?}", region);

            let mut base_url: BaseUrl = host.parse().unwrap();
            base_url.https = secure;
            base_url.region = region;

            let static_provider = StaticProvider::new(&access_key, &secret_key, None);
            let client = Client::new(
                base_url.clone(),
                Some(Box::new(static_provider)),
                Some(&*ssl_cert_file),
                Some(ignore_cert_check),
            )
            .unwrap();

            Self {
                base_url,
                access_key,
                secret_key,
                ignore_cert_check: Some(ignore_cert_check),
                ssl_cert_file: Some(ssl_cert_file),
                client,
            }
        }
    }
}

#[allow(dead_code)]
pub async fn create_bucket_helper(ctx: &TestContext) -> (String, CleanupGuard) {
    let bucket_name = rand_bucket_name();
    let _resp = ctx.client.make_bucket(&bucket_name).send().await.unwrap();
    let guard = CleanupGuard::new(ctx, &bucket_name);
    (bucket_name, guard)
}

// Cleanup guard that removes the bucket when it is dropped
pub struct CleanupGuard {
    ctx: TestContext,
    bucket_name: String,
}

impl CleanupGuard {
    #[allow(dead_code)]
    pub fn new(ctx: &TestContext, bucket_name: &str) -> Self {
        Self {
            ctx: ctx.clone(),
            bucket_name: bucket_name.to_string(),
        }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let ctx = self.ctx.clone();
        let bucket_name = self.bucket_name.clone();
        //println!("Going to remove bucket {}", bucket_name);

        // Spawn the cleanup task in a way that detaches it from the current runtime
        thread::spawn(move || {
            // Create a new runtime for this thread
            let rt = tokio::runtime::Runtime::new().unwrap();

            // Execute the async cleanup in this new runtime
            rt.block_on(async {
                // do the actual removal of the bucket
                match timeout(
                    std::time::Duration::from_secs(60),
                    ctx.client.remove_and_purge_bucket(&bucket_name),
                )
                .await
                {
                    Ok(result) => match result {
                        Ok(_) => {
                            //println!("Bucket {} removed successfully", bucket_name),
                        }
                        Err(e) => println!("Error removing bucket {}: {:?}", bucket_name, e),
                    },
                    Err(_) => println!("Timeout after 60s while removing bucket {}", bucket_name),
                }
            });
        })
        .join()
        .unwrap(); // This blocks the current thread until cleanup is done
    }
}
