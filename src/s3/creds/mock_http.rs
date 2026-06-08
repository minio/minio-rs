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

//! Minimal in-process HTTP server for exercising the network credential
//! providers (IMDS/ECS/STS) end-to-end in unit tests, mirroring the
//! `httptest`-based tests in minio-go's `credentials` package.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Parsed request: method, path, and lowercase-keyed headers.
pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: HashMap<String, String>,
}

/// Maps an incoming request to a `(status, body)` response.
pub(crate) type Responder = Arc<dyn Fn(&Request) -> (u16, String) + Send + Sync>;

/// A running mock HTTP server. The server stops when this is dropped.
pub(crate) struct MockServer {
    pub(crate) base_url: String,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Starts a mock server on an ephemeral loopback port. Each request is routed
/// through `responder`; the connection is closed after every response so the
/// client (reqwest) issues a fresh connection per request.
pub(crate) async fn start(responder: Responder) -> MockServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let responder = responder.clone();
            tokio::spawn(async move {
                let Some(request) = read_request(&mut sock).await else {
                    return;
                };
                let (status, body) = responder(&request);
                let reason = if (200..300).contains(&status) {
                    "OK"
                } else {
                    "Error"
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(response.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });

    MockServer { base_url, handle }
}

/// Reads a full request (headers and, when present, the Content-Length body) so
/// the client's write completes before the response is sent, then parses the
/// request line and headers.
async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<Request> {
    let mut data = Vec::new();
    let mut buf = [0u8; 1024];
    let header_end = loop {
        let n = sock.read(&mut buf).await.ok()?;
        if n == 0 {
            break find_header_end(&data)?;
        }
        data.extend_from_slice(&buf[..n]);

        if let Some(pos) = find_header_end(&data) {
            let header_text = String::from_utf8_lossy(&data[..pos]);
            let content_length = content_length(&header_text);
            let body_received = data.len() - (pos + 4);
            if body_received >= content_length {
                break pos;
            }
        }
    };

    let header_text = String::from_utf8_lossy(&data[..header_end]);
    let mut lines = header_text.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();

    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();

    Some(Request {
        method,
        path,
        headers,
    })
}

fn content_length(header_text: &str) -> usize {
    header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_header_end_locates_blank_line() {
        assert_eq!(find_header_end(b"AB\r\n\r\nCD"), Some(2));
        assert_eq!(find_header_end(b"no terminator\r\n"), None);
    }

    #[test]
    fn content_length_parses_when_present() {
        assert_eq!(
            content_length("POST / HTTP/1.1\r\nContent-Length: 42\r\n"),
            42
        );
        assert_eq!(content_length("content-length: 7"), 7);
        assert_eq!(content_length("GET / HTTP/1.1"), 0);
        assert_eq!(content_length("Content-Length: not-a-number"), 0);
    }

    #[tokio::test]
    async fn round_trip_routes_by_method_and_path() {
        let responder: Responder = Arc::new(|req: &Request| {
            if req.method == "POST" && req.path == "/echo" {
                (200, "ok".to_string())
            } else {
                (404, String::new())
            }
        });
        let server = start(responder).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{}/echo", server.base_url))
            .body("hello")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "ok");

        let resp = client
            .get(format!("{}/missing", server.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
    }
}
