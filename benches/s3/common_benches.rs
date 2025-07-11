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

use criterion::Criterion;
use minio::s3::MinioClient;
use minio::s3::error::Error;
use minio::s3::response::{CreateBucketResponse, PutObjectContentResponse};
use minio::s3::types::{FromS3Response, S3Api, S3Request};
use minio_common::cleanup_guard::CleanupGuard;
use minio_common::test_context::TestContext;
use minio_common::utils::{
    get_bytes_from_response, get_response_from_bytes, rand_bucket_name, rand_object_name,
};
use std::env;
use tokio::runtime::Runtime;

pub(crate) struct Ctx2 {
    pub client: MinioClient,
    pub bucket: String,
    pub object: String,
    _cleanup: CleanupGuard,
    pub aux_bucket: Option<String>,
    _aux_cleanup: Option<CleanupGuard>,
}

impl Ctx2 {
    /// Create a new context with a bucket
    pub async fn new() -> Self {
        unsafe {
            env::set_var("MINIO_SSL_CERT_FILE", "./tests/public.crt");
        }
        let ctx = TestContext::new_from_env();
        let (bucket_name, cleanup) = ctx.create_bucket_helper().await;

        Self {
            client: ctx.client,
            bucket: bucket_name,
            object: "".to_string(),
            _cleanup: cleanup,
            aux_bucket: None,
            _aux_cleanup: None,
        }
    }
    /// Create a new context with a bucket and an object
    pub async fn new_with_object(object_lock: bool) -> Self {
        unsafe {
            env::set_var("MINIO_SSL_CERT_FILE", "./tests/public.crt");
        }
        let ctx = TestContext::new_from_env();
        let bucket_name: String = rand_bucket_name();
        let _resp: CreateBucketResponse = ctx
            .client
            .create_bucket(&bucket_name)
            .object_lock(object_lock)
            .build()
            .send()
            .await
            .unwrap();
        let cleanup = CleanupGuard::new(ctx.client.clone(), &bucket_name);
        let object_name = rand_object_name();
        let data = bytes::Bytes::from("hello, world".to_string().into_bytes());
        let _resp: PutObjectContentResponse = ctx
            .client
            .put_object_content(&bucket_name, &object_name, data)
            .build()
            .send()
            .await
            .unwrap();

        Self {
            client: ctx.client,
            bucket: bucket_name,
            object: object_name.to_string(),
            _cleanup: cleanup,
            aux_bucket: None,
            _aux_cleanup: None,
        }
    }
    #[allow(dead_code)]
    pub async fn new_aux(&mut self) -> String {
        let bucket_name: String = rand_bucket_name();
        self.aux_bucket = Some(bucket_name.clone());
        self._aux_cleanup = Some(CleanupGuard::new(self.client.clone(), &bucket_name));
        let _resp: CreateBucketResponse = self
            .client
            .create_bucket(&bucket_name)
            .object_lock(false)
            .build()
            .send()
            .await
            .unwrap();

        bucket_name
    }
}

pub(crate) fn benchmark_s3_api<ApiType, GlobalSetupFuture>(
    name: &str,
    criterion: &mut Criterion,
    global_setup: impl Fn() -> GlobalSetupFuture,
    per_iter_setup: impl Fn(&Ctx2) -> ApiType,
) where
    ApiType: S3Api,
    GlobalSetupFuture: Future<Output = Ctx2>,
{
    let rt = Runtime::new().unwrap();
    let mut group = criterion.benchmark_group(name);

    // Global setup
    let ctx: Ctx2 = rt.block_on(global_setup());

    // Benchmark to_s3request phase
    group.bench_function("to_s3request", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let api = per_iter_setup(&ctx);

                let start = std::time::Instant::now();
                let _request = api.to_s3request();
                total += start.elapsed();
            }
            total
        })
    });

    // Benchmark from_s3response phase
    group.bench_function("from_s3response", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;

            // Per-iteration setup for initial request
            let api = per_iter_setup(&ctx);
            let request: S3Request = api.to_s3request().unwrap();

            // Execute the request to get a response, store the bytes for swift cloning
            let bytes: bytes::Bytes = rt.block_on(async {
                let resp: Result<reqwest::Response, Error> = request.clone().execute().await;
                get_bytes_from_response(resp).await
            });

            for _ in 0..iters {
                let response2 = Ok(get_response_from_bytes(bytes.clone()));
                let request2 = request.clone();

                let start = std::time::Instant::now();
                rt.block_on(async {
                    let _ =
                        <ApiType as S3Api>::S3Response::from_s3response(request2, response2).await;
                });
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

pub(crate) async fn skip_express_mode(bench_name: &str) -> bool {
    let skip = TestContext::new_from_env().client.is_minio_express().await;
    if skip {
        println!("Skipping benchmark '{bench_name}' (MinIO Express mode)");
    }
    skip
}
