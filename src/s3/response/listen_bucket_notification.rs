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

use crate::impl_has_s3fields;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, NotificationRecords, S3Request};
use async_std::stream::Stream;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use http::HeaderMap;
use std::mem;

/// Response of
/// [listen_bucket_notification()](crate::s3::client::MinioClient::listen_bucket_notification)
/// API
#[derive(Clone, Debug)]
pub struct ListenBucketNotificationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes, // Note: not used
}

impl_has_s3fields!(ListenBucketNotificationResponse);

impl HasBucket for ListenBucketNotificationResponse {}
impl HasRegion for ListenBucketNotificationResponse {}

#[async_trait::async_trait]
impl FromS3Response
    for (
        ListenBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    )
{
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let byte_stream = resp.bytes_stream();

        let line_stream = Box::pin(async_stream::try_stream! {
            let mut buf = Vec::new();
            let mut cursor = 0;

            let mut stream = byte_stream.map_err(ValidationErr::from).boxed();

            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                buf.extend_from_slice(&chunk);

                while let Some(pos) = buf[cursor..].iter().position(|&b| b == b'\n') {
                    let end = cursor + pos;
                    let line_bytes = &buf[..end];
                    let line = std::str::from_utf8(line_bytes).map_err(ValidationErr::from)?.trim();

                    if !line.is_empty() {
                        let parsed: NotificationRecords = serde_json::from_str(line).map_err(ValidationErr::from)?;
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
                let line = std::str::from_utf8(&buf).map_err(ValidationErr::from)?.trim();
                if !line.is_empty() {
                    let parsed: NotificationRecords = serde_json::from_str(line).map_err(ValidationErr::from)?;
                    yield parsed;
                }
            }

        });

        Ok((
            ListenBucketNotificationResponse {
                request,
                headers,
                body: Bytes::new(),
            },
            Box::new(line_stream),
        ))
    }
}
