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

use async_std::stream::StreamExt;
use async_std::task;
use minio::s3::builders::ObjectContent;
use minio::s3::response::PutObjectContentResponse;
use minio::s3::response::a_response_traits::{HasBucket, HasObject};
use minio::s3::types::{NotificationRecord, NotificationRecords, S3Api};
use minio_common::rand_src::RandSrc;
use minio_common::test_context::TestContext;
use minio_common::utils::rand_object_name;
use tokio::sync::mpsc;

#[minio_macros::test(flavor = "multi_thread", worker_threads = 10)]
async fn listen_bucket_notification(ctx: TestContext, bucket_name: String) {
    let object_name = rand_object_name();

    type MessageType = u32;
    const SECRET_MSG: MessageType = 42;

    let (sender, mut receiver): (
        mpsc::UnboundedSender<MessageType>,
        mpsc::UnboundedReceiver<MessageType>,
    ) = mpsc::unbounded_channel();

    let bucket_name2 = bucket_name.clone();
    let object_name2 = object_name.clone();

    let spawned_listen_task = task::spawn(async move {
        let ctx2 = TestContext::new_from_env();

        let (_resp, mut event_stream) = ctx2
            .client
            .listen_bucket_notification(&bucket_name2)
            .build()
            .send()
            .await
            .unwrap();

        while let Some(event) = event_stream.next().await {
            let event: NotificationRecords = event.unwrap();
            let record: Option<&NotificationRecord> = event.records.first();

            if let Some(record) = record {
                let key: &str = &record.s3.object.key;
                if key == object_name2 {
                    // Do something with the record, check if you received an event triggered
                    // by the put_object that will happen in a few ms.
                    assert_eq!(record.event_name, "s3:ObjectCreated:Put");
                    assert_eq!(record.s3.bucket.name, bucket_name2);
                    //println!("record {:#?}", record);

                    sender.send(SECRET_MSG).unwrap();
                    break;
                }
            }
            sender.send(0).unwrap();
        }
    });

    // wait a few ms to before we issue a put_object
    task::sleep(std::time::Duration::from_millis(200)).await;

    let size = 16_u64;
    let resp: PutObjectContentResponse = ctx
        .client
        .put_object_content(
            &bucket_name,
            &object_name,
            ObjectContent::new_from_stream(RandSrc::new(size), Some(size)),
        )
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.bucket(), bucket_name);
    assert_eq!(resp.object(), object_name);

    spawned_listen_task.await;

    let received_message: MessageType = receiver.recv().await.unwrap();
    assert_eq!(received_message, SECRET_MSG);
}
