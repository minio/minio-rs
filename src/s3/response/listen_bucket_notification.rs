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

use futures_util::{stream, Stream, StreamExt};
use http::HeaderMap;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

use crate::s3::{
    error::Error,
    types::{FromS3Response, NotificationRecords, S3Request},
};

/// Response of
/// [listen_bucket_notification()](crate::s3::client::Client::listen_bucket_notification)
/// API
#[derive(Debug)]
pub struct ListenBucketNotificationResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
}

#[async_trait::async_trait]
impl FromS3Response
    for (
        ListenBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    )
{
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();

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
            ListenBucketNotificationResponse {
                headers,
                region: req.get_computed_region(),
                bucket: req.bucket.unwrap().to_string(),
            },
            Box::new(record_stream),
        ))
    }
}
