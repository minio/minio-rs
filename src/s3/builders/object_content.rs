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

use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use futures_util::Stream;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;

type IoResult<T> = Result<T, std::io::Error>;

pub struct ObjectContent {
    r: Pin<Box<dyn Stream<Item = IoResult<Bytes>>>>,
    extra: Option<Bytes>,
    size: Option<u64>,
}

impl From<Bytes> for ObjectContent {
    fn from(value: Bytes) -> Self {
        let n = value.len();
        ObjectContent {
            r: Box::pin(tokio_stream::iter(vec![Ok(value)])),
            extra: None,
            size: Some(n as u64),
        }
    }
}

impl From<String> for ObjectContent {
    fn from(value: String) -> Self {
        let n = value.len();
        ObjectContent {
            r: Box::pin(tokio_stream::iter(vec![Ok(Bytes::from(value))])),
            extra: None,
            size: Some(n as u64),
        }
    }
}

impl From<Vec<u8>> for ObjectContent {
    fn from(value: Vec<u8>) -> Self {
        let n = value.len();
        ObjectContent {
            r: Box::pin(tokio_stream::iter(vec![Ok(Bytes::from(value))])),
            extra: None,
            size: Some(n as u64),
        }
    }
}

impl ObjectContent {
    pub fn new(r: impl Stream<Item = IoResult<Bytes>> + 'static, size: Option<u64>) -> Self {
        let r = Box::pin(r);
        Self {
            r,
            extra: None,
            size,
        }
    }

    pub fn empty() -> Self {
        Self {
            r: Box::pin(tokio_stream::iter(vec![])),
            extra: None,
            size: Some(0),
        }
    }

    pub fn from_reader(r: impl AsyncRead + Send + Sync + 'static, size: Option<u64>) -> Self {
        let pinned = Box::pin(r);
        let r = tokio_util::io::ReaderStream::new(pinned);
        Self {
            r: Box::pin(r),
            extra: None,
            size,
        }
    }

    pub fn get_size(&self) -> Option<u64> {
        self.size
    }

    pub fn stream(self) -> impl Stream<Item = IoResult<Bytes>> {
        self.r
    }

    // Read as many bytes as possible up to `n` and return a `SegmentedBytes`
    // object.
    pub async fn read_upto(&mut self, n: usize) -> IoResult<SegmentedBytes> {
        let mut segmented_bytes = SegmentedBytes::new();
        let mut remaining = n;
        if let Some(extra) = self.extra.take() {
            let len = extra.len();
            if len <= remaining {
                segmented_bytes.append(extra);
                remaining -= len;
            } else {
                segmented_bytes.append(extra.slice(0..remaining));
                self.extra = Some(extra.slice(remaining..));
                return Ok(segmented_bytes);
            }
        }
        while remaining > 0 {
            let bytes = self.r.next().await;
            if bytes.is_none() {
                break;
            }
            let bytes = bytes.unwrap()?;
            let len = bytes.len();
            if len == 0 {
                break;
            }
            if len <= remaining {
                segmented_bytes.append(bytes);
                remaining -= len;
            } else {
                segmented_bytes.append(bytes.slice(0..remaining));
                self.extra = Some(bytes.slice(remaining..));
                break;
            }
        }
        Ok(segmented_bytes)
    }

    pub async fn to_segmented_bytes(mut self) -> IoResult<SegmentedBytes> {
        let mut segmented_bytes = SegmentedBytes::new();
        while let Some(bytes) = self.r.next().await {
            let bytes = bytes?;
            if bytes.is_empty() {
                break;
            }
            segmented_bytes.append(bytes);
        }
        Ok(segmented_bytes)
    }
}

#[derive(Debug, Clone)]
pub struct SegmentedBytes {
    segments: Vec<Vec<Bytes>>,
    total_size: usize,
}

impl Default for SegmentedBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentedBytes {
    pub fn new() -> Self {
        SegmentedBytes {
            segments: Vec::new(),
            total_size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.total_size
    }

    pub fn append(&mut self, bytes: Bytes) {
        let last_segment = self.segments.last_mut();
        if let Some(last_segment) = last_segment {
            let last_len = last_segment.last().map(|b| b.len());
            if let Some(last_len) = last_len {
                if bytes.len() == last_len {
                    self.total_size += bytes.len();
                    last_segment.push(bytes);
                    return;
                }
            }
        }
        self.total_size += bytes.len();
        self.segments.push(vec![bytes]);
    }

    pub fn iter(&self) -> SegmentedBytesIterator {
        SegmentedBytesIterator {
            sb: self,
            current_segment: 0,
            current_segment_index: 0,
        }
    }

    // Copy all the content into a single `Bytes` object.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.total_size);
        for segment in &self.segments {
            for bytes in segment {
                buf.extend_from_slice(&bytes);
            }
        }
        buf.freeze()
    }
}

impl From<Bytes> for SegmentedBytes {
    fn from(bytes: Bytes) -> Self {
        let mut sb = SegmentedBytes::new();
        sb.append(bytes);
        sb
    }
}

pub struct SegmentedBytesIterator<'a> {
    sb: &'a SegmentedBytes,
    current_segment: usize,
    current_segment_index: usize,
}

impl Iterator for SegmentedBytesIterator<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_segment >= self.sb.segments.len() {
            return None;
        }
        let segment = &self.sb.segments[self.current_segment];
        if self.current_segment_index >= segment.len() {
            self.current_segment += 1;
            self.current_segment_index = 0;
            return self.next();
        }
        let bytes = &segment[self.current_segment_index];
        self.current_segment_index += 1;
        Some(bytes.clone())
    }
}
