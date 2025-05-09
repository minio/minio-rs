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

use futures_util::{Stream, TryStreamExt, stream};
use http::HeaderMap;
use std::mem;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

use crate::s3::utils::take_bucket;
use crate::s3::{
    error::Error,
    types::{FromS3Response, NotificationRecords, S3Request},
};

/// Response of
/// [list_bucket_notification()](crate::s3::client::Client::list_bucket_notification)
/// API
#[derive(Debug)]
pub struct ListBucketNotificationResponse {
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
        ListBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    )
{
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());

        let stream_reader = StreamReader::new(resp.bytes_stream().map_err(std::io::Error::other));

        let record_stream = Box::pin(stream::unfold(
            stream_reader,
            move |mut reader| async move {
                loop {
                    let mut line = String::new();
                    return match reader.read_line(&mut line).await {
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
                            Some((records_res, reader))
                        }
                        Err(e) => Some((Err(e.into()), reader)),
                    };
                }
            },
        ));

        Ok((
            ListBucketNotificationResponse {
                headers,
                region: req.inner_region,
                bucket: take_bucket(req.bucket)?,
            },
            Box::new(record_stream),
        ))
    }
}
