// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

//! MinIO Extension API for S3 Buckets: ListenBucketNotification

use crate::s3::builders::{ListenBucketNotification, ListenBucketNotificationBldr};
use crate::s3::client::MinioClient;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`ListenBucketNotification`] request builder.
    ///
    /// To execute the request, call [`ListenBucketNotification::send()`](crate::s3::types::S3Api::send),
    /// which returns a tuple of [`ListenBucketNotificationResponse`](crate::s3::response::ListenBucketNotificationResponse) and a
    /// stream of [`NotificationRecords`](crate::s3::types::NotificationRecords). The former contains the HTTP headers
    /// returned by the server and the latter is a stream of notification
    /// records. In normal operation (when there are no errors), the stream
    /// never ends.
    ///
    /// # MinIO Extensions
    ///
    /// This function is only available in MinIO and not part of the AWS S3 API.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::{BucketName, NotificationRecord, NotificationRecords, S3Api};
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let (_resp, mut event_stream) = client
    ///         .listen_bucket_notification(BucketName::new("bucket-name").unwrap())
    ///         .build().send().await.unwrap();
    ///
    ///     while let Some(event) = event_stream.next().await {
    ///         let event: NotificationRecords = event.unwrap();
    ///         let record: Option<&NotificationRecord> = event.records.first();
    ///         println!("received a notification record {:#?}", record);
    ///     }
    /// }
    /// ```
    pub fn listen_bucket_notification(&self, bucket: BucketName) -> ListenBucketNotificationBldr {
        ListenBucketNotification::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
