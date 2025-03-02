// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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
use http::header;
use hyper::http::Method;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::SmallRng;
use rand::SeedableRng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::{fs, io, thread};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::{Stream, StreamExt};

use minio::s3::args::*;
use minio::s3::builders::{ObjectContent, ObjectToDelete, VersioningStatus};
use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::error::Error;
use minio::s3::http::BaseUrl;
use minio::s3::response::GetBucketVersioningResponse;
use minio::s3::types::{
    CsvInputSerialization, CsvOutputSerialization, FileHeaderInfo, NotificationConfig,
    NotificationRecords, ObjectLockConfig, PrefixFilterRule, QueueConfig, QuoteFields,
    RetentionMode, S3Api, SelectRequest, SuffixFilterRule, ToStream,
};
use minio::s3::utils::{to_iso8601utc, utc_now};

struct RandReader {
    size: usize,
}

impl RandReader {
    fn new(size: usize) -> RandReader {
        RandReader { size }
    }
}

impl io::Read for RandReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let bytes_read = match self.size > buf.len() {
            true => buf.len(),
            false => self.size,
        };

        if bytes_read > 0 {
            let random: &mut dyn rand::RngCore = &mut rand::thread_rng();
            random.fill_bytes(&mut buf[0..bytes_read]);
        }

        self.size -= bytes_read;

        Ok(bytes_read)
    }
}

struct RandSrc {
    size: u64,
    rng: SmallRng,
}

impl RandSrc {
    fn new(size: u64) -> RandSrc {
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

fn rand_bucket_name() -> String {
    Alphanumeric
        .sample_string(&mut rand::thread_rng(), 8)
        .to_lowercase()
}

fn rand_object_name() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), 8)
}

#[derive(Clone)]
struct TestContext {
    base_url: BaseUrl,
    access_key: String,
    secret_key: String,
    ignore_cert_check: Option<bool>,
    ssl_cert_file: Option<PathBuf>,
    client: Client,
}

impl TestContext {
    const SQS_ARN: &'static str = "arn:minio:sqs::miniojavatest:webhook";

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

async fn create_bucket_helper(ctx: &TestContext) -> (String, CleanupGuard) {
    let bucket_name = rand_bucket_name();
    ctx.client
        .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    let guard = CleanupGuard::new(ctx, &bucket_name);
    (bucket_name, guard)
}

// Cleanup guard that removes the bucket when it is dropped
struct CleanupGuard {
    ctx: TestContext,
    bucket_name: String,
}

impl CleanupGuard {
    fn new(ctx: &TestContext, bucket_name: &str) -> Self {
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
                    ctx.client
                        .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap()),
                )
                .await
                {
                    Ok(result) => match result {
                        Ok(_) => {
                            //println!("Bucket {} removed successfully", bucket_name),
                        }
                        Err(e) => println!("Error removing bucket {}: {:?}", bucket_name, e),
                    },
                    Err(_) => println!("Timeout after 15s while removing bucket {}", bucket_name),
                }
            });
        })
        .join()
        .unwrap(); // This blocks the current thread until cleanup is done
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn bucket_exists() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let exists = ctx
        .client
        .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    assert!(exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_buckets() {
    const N_BUCKETS: usize = 3;
    let ctx = TestContext::new_from_env();

    let mut names: Vec<String> = Vec::new();
    let mut guards: Vec<CleanupGuard> = Vec::new();
    for _ in 1..=N_BUCKETS {
        let (bucket_name, guard) = create_bucket_helper(&ctx).await;
        names.push(bucket_name);
        guards.push(guard);
    }

    assert_eq!(names.len(), N_BUCKETS);

    let mut count = 0;
    let resp = ctx.client.list_buckets().send().await.unwrap();

    for bucket in resp.buckets.iter() {
        if names.contains(&bucket.name) {
            count += 1;
        }
    }
    assert_eq!(guards.len(), N_BUCKETS);
    assert_eq!(count, N_BUCKETS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn create_delete_bucket() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    ctx.client
        .make_bucket(&MakeBucketArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let exists = ctx
        .client
        .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(exists);

    ctx.client
        .remove_bucket(&RemoveBucketArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let exists = ctx
        .client
        .bucket_exists(&BucketExistsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(!exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.bucket_name, bucket_name);
    assert_eq!(resp.object_name, object_name);
    assert_eq!(resp.size, size);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    // Validate delete succeeded.
    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await;
    match resp.err().unwrap() {
        Error::S3Error(er) => {
            assert_eq!(er.code, "NoSuchKey")
        }
        e => panic!("Unexpected error {:?}", e),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object_multipart() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size: usize = 16 + 5 * 1024 * 1024;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.bucket_name, bucket_name);
    assert_eq!(resp.object_name, object_name);
    assert_eq!(resp.size, size);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object_content() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let sizes = [16_u64, 5 * 1024 * 1024, 16 + 5 * 1024 * 1024];

    for size in sizes.iter() {
        let data_src = RandSrc::new(*size);
        let rsp = ctx
            .client
            .put_object_content(
                &bucket_name,
                &object_name,
                ObjectContent::new_from_stream(data_src, Some(*size)),
            )
            .content_type(String::from("image/jpeg"))
            .send()
            .await
            .unwrap();
        assert_eq!(rsp.object_size, *size);
        let etag = rsp.etag;
        let resp = ctx
            .client
            .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.size, *size as usize);
        assert_eq!(resp.etag, etag);
        assert_eq!(
            resp.headers.get(header::CONTENT_TYPE).unwrap(),
            "image/jpeg"
        );
        ctx.client
            .remove_object(&bucket_name, object_name.as_str())
            .send()
            .await
            .unwrap();
    }

    // Repeat test with no size specified in ObjectContent
    for size in sizes.iter() {
        let data_src = RandSrc::new(*size);
        let rsp = ctx
            .client
            .put_object_content(
                &bucket_name,
                &object_name,
                ObjectContent::new_from_stream(data_src, None),
            )
            .part_size(Some(5 * 1024 * 1024)) // Set part size to 5MB
            .send()
            .await
            .unwrap();
        assert_eq!(rsp.object_size, *size);
        let etag = rsp.etag;
        let resp = ctx
            .client
            .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.size, *size as usize);
        assert_eq!(resp.etag, etag);
        ctx.client
            .remove_object(&bucket_name, object_name.as_str())
            .send()
            .await
            .unwrap();
    }
}

/// Test sending ObjectContent across async tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn put_object_content_2() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();
    let sizes = vec![16_u64, 5 * 1024 * 1024, 16 + 5 * 1024 * 1024];

    let (sender, mut receiver): (mpsc::Sender<ObjectContent>, mpsc::Receiver<ObjectContent>) =
        mpsc::channel(2);

    let sender_handle = {
        let sizes = sizes.clone();
        tokio::spawn(async move {
            for size in sizes.iter() {
                let data_src = RandSrc::new(*size);
                sender
                    .send(ObjectContent::new_from_stream(data_src, Some(*size)))
                    .await
                    .unwrap();
            }
        })
    };

    let uploader_handler = {
        let sizes = sizes.clone();
        let object_name = object_name.clone();
        let client = ctx.client.clone();
        let test_bucket = bucket_name.clone();
        tokio::spawn(async move {
            let mut idx = 0;
            while let Some(item) = receiver.recv().await {
                let rsp = client
                    .put_object_content(&test_bucket, &object_name, item)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(rsp.object_size, sizes[idx]);
                let etag = rsp.etag;
                let resp = client
                    .stat_object(&StatObjectArgs::new(&test_bucket, &object_name).unwrap())
                    .await
                    .unwrap();
                assert_eq!(resp.size, sizes[idx] as usize);
                assert_eq!(resp.etag, etag);
                client
                    .remove_object(&test_bucket, object_name.as_str())
                    .send()
                    .await
                    .unwrap();

                idx += 1;
            }
        })
    };

    sender_handle.await.unwrap();
    uploader_handler.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_object_old() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let data = "hello, world";
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut BufReader::new(data.as_bytes()),
                Some(data.len()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let resp = ctx
        .client
        .get_object_old(&GetObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    let got = resp.text().await.unwrap();
    assert_eq!(got, data);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let data = Bytes::from("hello, world".to_string().into_bytes());
    ctx.client
        .put_object_content(&bucket_name, &object_name, data.clone())
        .send()
        .await
        .unwrap();
    let resp = ctx
        .client
        .get_object(&bucket_name, &object_name)
        .send()
        .await
        .unwrap();
    let got = resp.content.to_segmented_bytes().await.unwrap().to_bytes();
    assert_eq!(got, data);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

fn get_hash(filename: &String) -> String {
    let mut hasher = Sha256::new();
    let mut file = fs::File::open(filename).unwrap();
    io::copy(&mut file, &mut hasher).unwrap();
    format!("{:x}", hasher.finalize())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn upload_download_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size = 16_usize;
    let mut file = fs::File::create(&object_name).unwrap();
    io::copy(&mut RandReader::new(size), &mut file).unwrap();
    file.sync_all().unwrap();
    ctx.client
        .upload_object(&UploadObjectArgs::new(&bucket_name, &object_name, &object_name).unwrap())
        .await
        .unwrap();

    let filename = rand_object_name();
    ctx.client
        .download_object(&DownloadObjectArgs::new(&bucket_name, &object_name, &filename).unwrap())
        .await
        .unwrap();
    assert_eq!(get_hash(&object_name), get_hash(&filename));

    fs::remove_file(&object_name).unwrap();
    fs::remove_file(&filename).unwrap();

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();

    let object_name = rand_object_name();
    let size: usize = 16 + 5 * 1024 * 1024;
    let mut file = fs::File::create(&object_name).unwrap();
    io::copy(&mut RandReader::new(size), &mut file).unwrap();
    file.sync_all().unwrap();
    ctx.client
        .upload_object(&UploadObjectArgs::new(&bucket_name, &object_name, &object_name).unwrap())
        .await
        .unwrap();

    let filename = rand_object_name();
    ctx.client
        .download_object(&DownloadObjectArgs::new(&bucket_name, &object_name, &filename).unwrap())
        .await
        .unwrap();
    assert_eq!(get_hash(&object_name), get_hash(&filename));

    fs::remove_file(&object_name).unwrap();
    fs::remove_file(&filename).unwrap();

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn remove_objects() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let mut names: Vec<String> = Vec::new();
    for _ in 1..=3 {
        let object_name = rand_object_name();
        let size = 0_usize;
        ctx.client
            .put_object_old(
                &mut PutObjectArgs::new(
                    &bucket_name,
                    &object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        names.push(object_name);
    }
    let del_items: Vec<ObjectToDelete> = names
        .iter()
        .map(|v| ObjectToDelete::from(v.as_str()))
        .collect();

    let mut resp = ctx
        .client
        .remove_objects(&bucket_name, del_items.into_iter())
        .verbose_mode(true)
        .to_stream()
        .await;

    let mut del_count = 0;
    while let Some(item) = resp.next().await {
        let res = item.unwrap();
        for obj in res.result.iter() {
            assert!(obj.is_deleted());
        }
        del_count += res.result.len();
    }
    assert_eq!(del_count, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn list_objects() {
    const N_OBJECTS: usize = 3;
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let mut names: Vec<String> = Vec::new();
    for _ in 1..=N_OBJECTS {
        let object_name = rand_object_name();
        let size = 0_usize;
        ctx.client
            .put_object_old(
                &mut PutObjectArgs::new(
                    &bucket_name,
                    &object_name,
                    &mut RandReader::new(size),
                    Some(size),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        names.push(object_name);
    }

    let mut stream = ctx.client.list_objects(&bucket_name).to_stream().await;

    let mut count = 0;
    while let Some(items) = stream.next().await {
        let items = items.unwrap().contents;
        for item in items.iter() {
            assert!(names.contains(&item.name));
            count += 1;
        }
    }
    assert_eq!(count, N_OBJECTS);

    let del_items: Vec<ObjectToDelete> = names
        .iter()
        .map(|v| ObjectToDelete::from(v.as_str()))
        .collect();
    let mut resp = ctx
        .client
        .remove_objects(&bucket_name, del_items.into_iter())
        .verbose_mode(true)
        .to_stream()
        .await;
    while let Some(item) = resp.next().await {
        let res = item.unwrap();
        for obj in res.result.iter() {
            assert!(obj.is_deleted());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn select_object_content() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let mut data = String::new();
    data.push_str("1997,Ford,E350,\"ac, abs, moon\",3000.00\n");
    data.push_str("1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00\n");
    data.push_str("1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00\n");
    data.push_str("1996,Jeep,Grand Cherokee,\"MUST SELL!\n");
    data.push_str("air, moon roof, loaded\",4799.00\n");
    let body = String::from("Year,Make,Model,Description,Price\n") + &data;

    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut BufReader::new(body.as_bytes()),
                Some(body.len()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let request = SelectRequest::new_csv_input_output(
        "select * from S3Object",
        CsvInputSerialization {
            compression_type: None,
            allow_quoted_record_delimiter: false,
            comments: None,
            field_delimiter: None,
            file_header_info: Some(FileHeaderInfo::USE),
            quote_character: None,
            quote_escape_character: None,
            record_delimiter: None,
        },
        CsvOutputSerialization {
            field_delimiter: None,
            quote_character: None,
            quote_escape_character: None,
            quote_fields: Some(QuoteFields::ASNEEDED),
            record_delimiter: None,
        },
    )
    .unwrap();
    let mut resp = ctx
        .client
        .select_object_content(
            &SelectObjectContentArgs::new(&bucket_name, &object_name, &request).unwrap(),
        )
        .await
        .unwrap();
    let mut got = String::new();
    let mut buf = [0_u8; 512];
    loop {
        let size = resp.read(&mut buf).await.unwrap();
        if size == 0 {
            break;
        }
        got += core::str::from_utf8(&buf[..size]).unwrap();
    }
    assert_eq!(got, data);
    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn listen_bucket_notification() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let name = object_name.clone();
    let (sender, mut receiver): (mpsc::UnboundedSender<bool>, mpsc::UnboundedReceiver<bool>) =
        mpsc::unbounded_channel();

    let access_key = ctx.access_key.clone();
    let secret_key = ctx.secret_key.clone();
    let base_url = ctx.base_url.clone();
    let ignore_cert_check = ctx.ignore_cert_check;
    let ssl_cert_file = ctx.ssl_cert_file.clone();
    let test_bucket = bucket_name.clone();

    let listen_task = move || async move {
        let static_provider = StaticProvider::new(&access_key, &secret_key, None);
        let ssl_cert_file = &ssl_cert_file;
        let client = Client::new(
            base_url,
            Some(Box::new(static_provider)),
            ssl_cert_file.as_deref(),
            ignore_cert_check,
        )
        .unwrap();

        let event_fn = |event: NotificationRecords| {
            let record = event.records.first();
            if let Some(record) = record {
                let key = &record.s3.object.key;
                if name == *key {
                    sender.send(true).unwrap();
                    return false;
                }
            }
            sender.send(false).unwrap();
            false
        };

        let (_, mut event_stream) = client
            .listen_bucket_notification(&test_bucket)
            .send()
            .await
            .unwrap();
        while let Some(event) = event_stream.next().await {
            let event = event.unwrap();
            if !event_fn(event) {
                break;
            }
        }
    };

    let spawned_task = task::spawn(listen_task());
    task::sleep(std::time::Duration::from_millis(200)).await;

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();

    spawned_task.await;
    assert!(receiver.recv().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn copy_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let src_object_name = rand_object_name();

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &src_object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let object_name = rand_object_name();
    ctx.client
        .copy_object(
            &CopyObjectArgs::new(
                &bucket_name,
                &object_name,
                CopySource::new(&bucket_name, &src_object_name).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.size, size);

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    ctx.client
        .remove_object(&bucket_name, src_object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn compose_object() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let src_object_name = rand_object_name();

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &src_object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut s1 = ComposeSource::new(&bucket_name, &src_object_name).unwrap();
    s1.offset = Some(3);
    s1.length = Some(5);
    let mut sources: Vec<ComposeSource> = Vec::new();
    sources.push(s1);

    let object_name = rand_object_name();

    ctx.client
        .compose_object(
            &mut ComposeObjectArgs::new(&bucket_name, &object_name, &mut sources).unwrap(),
        )
        .await
        .unwrap();

    let resp = ctx
        .client
        .stat_object(&StatObjectArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.size, 5);

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
    ctx.client
        .remove_object(&bucket_name, src_object_name.as_str())
        .send()
        .await
        .unwrap();
}

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
                        queue: String::from(TestContext::SQS_ARN),
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
        TestContext::SQS_ARN
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

    ctx.client
        .set_bucket_policy(&bucket_name)
        .config(config)
        .send()
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_policy(&bucket_name)
        .send()
        .await
        .unwrap();

    assert!(!resp.config.is_empty());

    ctx.client
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

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_bucket_tags() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    ctx.client
        .set_bucket_tags(&SetBucketTagsArgs::new(&bucket_name, &tags).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_tags(&GetBucketTagsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.len() == tags.len() && resp.tags.keys().all(|k| tags.contains_key(k)));

    ctx.client
        .delete_bucket_tags(&DeleteBucketTagsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_bucket_tags(&GetBucketTagsArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_object_lock_config() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    let mut args = MakeBucketArgs::new(&bucket_name).unwrap();
    args.object_lock = true;
    ctx.client.make_bucket(&args).await.unwrap();
    let _cleanup = CleanupGuard::new(&ctx, &bucket_name);

    ctx.client
        .set_object_lock_config(
            &SetObjectLockConfigArgs::new(
                &bucket_name,
                &ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(7), None).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_object_lock_config(&GetObjectLockConfigArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(match resp.config.retention_mode {
        Some(r) => matches!(r, RetentionMode::GOVERNANCE),
        _ => false,
    });

    assert_eq!(resp.config.retention_duration_days, Some(7));
    assert!(resp.config.retention_duration_years.is_none());

    ctx.client
        .delete_object_lock_config(&DeleteObjectLockConfigArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_object_lock_config(&GetObjectLockConfigArgs::new(&bucket_name).unwrap())
        .await
        .unwrap();
    assert!(resp.config.retention_mode.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_delete_object_tags() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;
    let object_name = rand_object_name();

    let size = 16_usize;
    ctx.client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let tags = HashMap::from([
        (String::from("Project"), String::from("Project One")),
        (String::from("User"), String::from("jsmith")),
    ]);

    ctx.client
        .set_object_tags(&SetObjectTagsArgs::new(&bucket_name, &object_name, &tags).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_object_tags(&GetObjectTagsArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.len() == tags.len() && resp.tags.keys().all(|k| tags.contains_key(k)));

    ctx.client
        .delete_object_tags(&DeleteObjectTagsArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();

    let resp = ctx
        .client
        .get_object_tags(&GetObjectTagsArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(resp.tags.is_empty());

    ctx.client
        .remove_object(&bucket_name, object_name.as_str())
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_bucket_versioning() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    ctx.client
        .set_bucket_versioning(&bucket_name)
        .versioning_status(VersioningStatus::Enabled)
        .send()
        .await
        .unwrap();

    let resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status, Some(VersioningStatus::Enabled));

    ctx.client
        .set_bucket_versioning(&bucket_name)
        .versioning_status(VersioningStatus::Suspended)
        .send()
        .await
        .unwrap();

    let resp: GetBucketVersioningResponse = ctx
        .client
        .get_bucket_versioning(&bucket_name)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status, Some(VersioningStatus::Suspended));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn set_get_object_retention() {
    let ctx = TestContext::new_from_env();
    let bucket_name = rand_bucket_name();

    let mut args = MakeBucketArgs::new(&bucket_name).unwrap();
    args.object_lock = true;
    ctx.client.make_bucket(&args).await.unwrap();
    let _cleanup = CleanupGuard::new(&ctx, &bucket_name);

    let object_name = rand_object_name();

    let size = 16_usize;
    let obj_resp = ctx
        .client
        .put_object_old(
            &mut PutObjectArgs::new(
                &bucket_name,
                &object_name,
                &mut RandReader::new(size),
                Some(size),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let retain_until_date = utc_now() + chrono::Duration::days(1);
    let args = SetObjectRetentionArgs::new(
        &bucket_name,
        &object_name,
        Some(RetentionMode::GOVERNANCE),
        Some(retain_until_date),
    )
    .unwrap();

    ctx.client.set_object_retention(&args).await.unwrap();

    let resp = ctx
        .client
        .get_object_retention(&GetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(match resp.retention_mode {
        Some(v) => matches!(v, RetentionMode::GOVERNANCE),
        _ => false,
    });
    assert!(match resp.retain_until_date {
        Some(v) => to_iso8601utc(v) == to_iso8601utc(retain_until_date),
        _ => false,
    },);

    let mut args = SetObjectRetentionArgs::new(&bucket_name, &object_name, None, None).unwrap();
    args.bypass_governance_mode = true;
    ctx.client.set_object_retention(&args).await.unwrap();

    let resp = ctx
        .client
        .get_object_retention(&GetObjectRetentionArgs::new(&bucket_name, &object_name).unwrap())
        .await
        .unwrap();
    assert!(resp.retention_mode.is_none());
    assert!(resp.retain_until_date.is_none());

    ctx.client
        .remove_object(
            &bucket_name,
            (object_name.as_str(), obj_resp.version_id.as_deref()),
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_presigned_object_url() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let object_name = rand_object_name();
    let resp = ctx
        .client
        .get_presigned_object_url(
            &GetPresignedObjectUrlArgs::new(&bucket_name, &object_name, Method::GET).unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.url.contains("X-Amz-Signature="));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn get_presigned_post_form_data() {
    let ctx = TestContext::new_from_env();
    let (bucket_name, _cleanup) = create_bucket_helper(&ctx).await;

    let object_name = rand_object_name();
    let expiration = utc_now() + chrono::Duration::days(5);

    let mut policy = PostPolicy::new(&bucket_name, &expiration).unwrap();
    policy.add_equals_condition("key", &object_name).unwrap();
    policy
        .add_content_length_range_condition(1024 * 1024, 4 * 1024 * 1024)
        .unwrap();

    let form_data = ctx
        .client
        .get_presigned_post_form_data(&policy)
        .await
        .unwrap();
    assert!(form_data.contains_key("x-amz-signature"));
    assert!(form_data.contains_key("policy"));
}
