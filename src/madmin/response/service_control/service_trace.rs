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

use crate::madmin::types::trace::ServiceTraceInfo;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use futures_util::stream::{self, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Response from service trace operation
///
/// This response provides a stream of trace events from the MinIO server.
/// Each event is a [`ServiceTraceInfo`] containing trace data.
pub struct ServiceTraceResponse {
    stream: Pin<Box<dyn Stream<Item = Result<ServiceTraceInfo, Error>> + Send>>,
}

impl ServiceTraceResponse {
    /// Convert the response into a stream of trace events
    pub fn into_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<ServiceTraceInfo, Error>> + Send>> {
        self.stream
    }
}

impl Stream for ServiceTraceResponse {
    type Item = Result<ServiceTraceInfo, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

//TODO please double check with S3 hwo streaming responses are handled, how dd API calls in s3 handle the request?
#[async_trait]
impl FromMadminResponse for ServiceTraceResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;

        let byte_stream = resp.bytes_stream();

        let stream = byte_stream
            .map(|result| result.map_err(|e| ValidationErr::HttpError(e).into()))
            .scan(
                Vec::new(),
                |buffer: &mut Vec<u8>, chunk_result: Result<bytes::Bytes, Error>| {
                    let buffer = std::mem::take(buffer);
                    async move {
                        match chunk_result {
                            Ok(chunk) => {
                                let mut buffer = buffer;
                                buffer.extend_from_slice(&chunk);

                                let mut events = Vec::new();

                                while let Some(newline_pos) =
                                    buffer.iter().position(|&b| b == b'\n')
                                {
                                    let line_bytes: Vec<u8> =
                                        buffer.drain(..=newline_pos).collect();

                                    if line_bytes.len() <= 1 {
                                        continue;
                                    }

                                    let line = match std::str::from_utf8(
                                        &line_bytes[..line_bytes.len() - 1],
                                    ) {
                                        Ok(s) => s.trim(),
                                        Err(_) => continue,
                                    };

                                    if line.is_empty() {
                                        continue;
                                    }

                                    match serde_json::from_str::<ServiceTraceInfo>(line) {
                                        Ok(event) => events.push(Ok(event)),
                                        Err(e) => {
                                            events.push(Err(ValidationErr::JsonError(e).into()))
                                        }
                                    }
                                }

                                Some((
                                    buffer,
                                    if events.is_empty() {
                                        None
                                    } else {
                                        Some(stream::iter(events))
                                    },
                                ))
                            }
                            Err(e) => Some((buffer, Some(stream::iter(vec![Err(e)])))),
                        }
                    }
                },
            )
            .filter_map(|(buffer, events)| async move { events.map(|e| (buffer, e)) })
            .map(|(_, events)| events)
            .flatten();

        Ok(ServiceTraceResponse {
            stream: Box::pin(stream),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_trace_line() {
        let json = r#"{"type":"http","nodename":"server1:9000","funcname":"PutObject","time":"2025-01-01T00:00:00Z","path":"/bucket/object","dur":1000000}"#;
        let info: ServiceTraceInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.trace.node_name, "server1:9000");
    }
}
