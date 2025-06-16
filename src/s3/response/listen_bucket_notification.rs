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

use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, NotificationRecords, S3Request};
use crate::s3::utils::take_bucket;
use futures_util::{Stream, StreamExt, TryStreamExt};
use http::HeaderMap;
use std::mem;

/// Response of
/// [listen _bucket_notification()](crate::s3::client::Client::listen_bucket_notification)
/// API
#[derive(Debug)]
pub struct ListenBucketNotificationResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,
}

#[async_trait::async_trait]
impl FromS3Response
    for (
        ListenBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    )
{
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());

        // A simple stateful decoder that buffers bytes and yields complete lines
        let byte_stream = resp.bytes_stream(); // This is a futures::Stream<Item = Result<Bytes, reqwest::Error>>

        let line_stream = Box::pin(async_stream::try_stream! {
            let mut buf = Vec::new();
            let mut cursor = 0;

            let mut stream = byte_stream.map_err(Error::from).boxed();

            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                buf.extend_from_slice(&chunk);

                while let Some(pos) = buf[cursor..].iter().position(|&b| b == b'\n') {
                    let end = cursor + pos;
                    let line_bytes = &buf[..end];
                    let line = std::str::from_utf8(line_bytes)?.trim();

                    if !line.is_empty() {
                        let parsed: NotificationRecords = serde_json::from_str(line)?;
                        yield parsed;
                    }

                    cursor = end + 1;
                }

                // Shift buffer left if needed
                if cursor > 0 {
                    buf.drain(..cursor);
                    cursor = 0;
                }
            }

            // Drain the remaining buffer if not empty
            if !buf.is_empty() {
                let line = std::str::from_utf8(&buf)?.trim();
                if !line.is_empty() {
                    let parsed: NotificationRecords = serde_json::from_str(line)?;
                    yield parsed;
                }
            }
        });

        Ok((
            ListenBucketNotificationResponse {
                headers,
                region: req.inner_region,
                bucket: take_bucket(req.bucket)?,
            },
            Box::new(line_stream),
        ))
    }
}
