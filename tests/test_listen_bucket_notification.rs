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

mod common;

use crate::common::{TestContext, create_bucket_helper, rand_object_name};
use async_std::task;
use common::RandSrc;
use minio::s3::Client;
use minio::s3::builders::ObjectContent;
use minio::s3::creds::StaticProvider;
use minio::s3::types::{NotificationRecords, S3Api};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

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

    let size = 16_u64;
    ctx.client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .send()
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
