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

use crate::madmin::types::performance::SpeedTestResult;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use futures_util::stream::{self, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Response from speedtest operation
///
/// This response provides a stream of speed test results from the MinIO server.
/// Each event is a [`SpeedTestResult`] containing performance metrics.
pub struct SpeedtestResponse {
    stream: Pin<Box<dyn Stream<Item = Result<SpeedTestResult, Error>> + Send>>,
}

impl SpeedtestResponse {
    /// Convert the response into a stream of speed test results
    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = Result<SpeedTestResult, Error>> + Send>> {
        self.stream
    }
}

impl Stream for SpeedtestResponse {
    type Item = Result<SpeedTestResult, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

#[async_trait]
impl FromMadminResponse for SpeedtestResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;

        let stream = resp
            .bytes_stream()
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

                                    match serde_json::from_str::<SpeedTestResult>(line) {
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

        Ok(SpeedtestResponse {
            stream: Box::pin(stream),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_speedtest_result() {
        let json = r#"{"version":"v1","servers":3,"disks":12,"size":1048576,"concurrent":10,"PUTStats":{"throughputPerSec":1048576000,"objectsPerSec":1000,"responseTime":{"avg":10000000,"p50":9000000,"p75":11000000,"p95":15000000,"p99":20000000,"p999":25000000,"l5p":22000000,"s5p":8000000,"max":30000000,"min":5000000,"sdev":5000000,"range":25000000},"servers":[]},"GETStats":{"throughputPerSec":2097152000,"objectsPerSec":2000,"responseTime":{"avg":5000000,"p50":4500000,"p75":5500000,"p95":7500000,"p99":10000000,"p999":12500000,"l5p":11000000,"s5p":4000000,"max":15000000,"min":2500000,"sdev":2500000,"range":12500000},"servers":[]}}"#;
        let result: SpeedTestResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.version, "v1");
        assert_eq!(result.servers, 3);
        assert_eq!(result.disks, 12);
    }
}
