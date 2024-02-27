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

use futures_util::stream;
use http::Method;
use tokio::io::AsyncBufReadExt;
use tokio_stream::{Stream, StreamExt};
use tokio_util::io::StreamReader;

use crate::s3::{
    args::ListenBucketNotificationArgs,
    error::Error,
    response::ListenBucketNotificationResponse,
    types::NotificationRecords,
    utils::{merge, Multimap},
};

use super::Client;

impl Client {
    /// Listens for bucket notifications. This is MinIO extension API. This
    /// function returns a tuple of `ListenBucketNotificationResponse` and a
    /// stream of `NotificationRecords`. The former contains the HTTP headers
    /// returned by the server and the latter is a stream of notification
    /// records. In normal operation (when there are no errors), the stream
    /// never ends.
    pub async fn listen_bucket_notification(
        &self,
        args: ListenBucketNotificationArgs,
    ) -> Result<
        (
            ListenBucketNotificationResponse,
            impl Stream<Item = Result<NotificationRecords, Error>>,
        ),
        Error,
    > {
        if self.base_url.is_aws_host() {
            return Err(Error::UnsupportedApi(String::from(
                "ListenBucketNotification",
            )));
        }

        let region = self
            .get_region(&args.bucket, args.region.as_deref())
            .await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.prefix {
            query_params.insert(String::from("prefix"), v.to_string());
        }
        if let Some(v) = args.suffix {
            query_params.insert(String::from("suffix"), v.to_string());
        }
        if let Some(v) = &args.events {
            for e in v.iter() {
                query_params.insert(String::from("events"), e.to_string());
            }
        } else {
            query_params.insert(String::from("events"), String::from("s3:ObjectCreated:*"));
            query_params.insert(String::from("events"), String::from("s3:ObjectRemoved:*"));
            query_params.insert(String::from("events"), String::from("s3:ObjectAccessed:*"));
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;

        let header_map = resp.headers().clone();

        let body_stream = resp.bytes_stream();
        let body_stream = body_stream
            .map(|r| r.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)));
        let stream_reader = StreamReader::new(body_stream);

        let record_stream = Box::pin(stream::unfold(
            stream_reader,
            move |mut reader| async move {
                loop {
                    let mut line = String::new();
                    match reader.read_line(&mut line).await {
                        Ok(n) => {
                            if n == 0 {
                                return None;
                            }
                            let s = line.trim();
                            if s.is_empty() {
                                continue;
                            }
                            let records_res: Result<NotificationRecords, Error> =
                                serde_json::from_str(s).map_err(|e| e.into());
                            return Some((records_res, reader));
                        }
                        Err(e) => return Some((Err(e.into()), reader)),
                    }
                }
            },
        ));

        Ok((
            ListenBucketNotificationResponse::new(header_map, &region, &args.bucket),
            record_stream,
        ))
    }
}
