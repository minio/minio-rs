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

use bytes::{Bytes, BytesMut};
use std::fmt;

/// An aggregated collection of `Bytes` objects.
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
        Self {
            segments: Vec::new(),
            total_size: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.total_size
    }

    pub fn is_empty(&self) -> bool {
        self.total_size == 0
    }

    pub fn append(&mut self, bytes: Bytes) {
        self.total_size += bytes.len();
        if let Some(last_segment) = self.segments.last_mut() {
            last_segment.push(bytes);
        } else {
            self.segments.push(vec![bytes]);
        }
    }

    pub fn iter(&self) -> SegmentedBytesIterator<'_> {
        SegmentedBytesIterator {
            sb: self,
            current_segment: 0,
            current_segment_index: 0,
        }
    }

    /// Copy all the content into a single [Bytes] object.
    ///
    /// ⚠️ This function is slow and intended for testing/debugging only.
    /// Do not use in performance-critical code.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.total_size);
        for segment in &self.segments {
            for bytes in segment {
                buf.extend_from_slice(bytes);
            }
        }
        buf.freeze()
    }
}

impl fmt::Display for SegmentedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.to_bytes().as_ref()) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => Ok(()), // or: write!(f, "<invalid utf8>")
        }
    }
}

pub struct SegmentedBytesIntoIterator {
    sb: SegmentedBytes,
    current_segment: usize,
    current_segment_index: usize,
}

impl Iterator for SegmentedBytesIntoIterator {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_segment >= self.sb.segments.len() {
            return None;
        }
        let segment: &Vec<Self::Item> = &self.sb.segments[self.current_segment];
        if self.current_segment_index >= segment.len() {
            self.current_segment += 1;
            self.current_segment_index = 0;
            return self.next();
        }
        let bytes: Self::Item = segment[self.current_segment_index].clone(); // Note: clone of Bytes does not make a deep copy of byte
        self.current_segment_index += 1;
        Some(bytes)
    }
}

impl IntoIterator for SegmentedBytes {
    type Item = Bytes;
    type IntoIter = SegmentedBytesIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        SegmentedBytesIntoIterator {
            sb: self,
            current_segment: 0,
            current_segment_index: 0,
        }
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
        let segment: &Vec<Self::Item> = &self.sb.segments[self.current_segment];
        if self.current_segment_index >= segment.len() {
            self.current_segment += 1;
            self.current_segment_index = 0;
            return Iterator::next(self);
        }
        let bytes: Self::Item = segment[self.current_segment_index].clone(); // Note: clone of Bytes does not make a deep copy of byte
        self.current_segment_index += 1;
        Some(bytes)
    }
}

impl<'a> IntoIterator for &'a SegmentedBytes {
    type Item = Bytes;
    type IntoIter = SegmentedBytesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SegmentedBytesIterator {
            sb: self,
            current_segment: 0,
            current_segment_index: 0,
        }
    }
}

impl From<Bytes> for SegmentedBytes {
    fn from(bytes: Bytes) -> Self {
        let total_size = bytes.len();
        Self {
            segments: vec![vec![bytes]],
            total_size,
        }
    }
}

impl From<String> for SegmentedBytes {
    fn from(s: String) -> Self {
        let total_size = s.len(); // take number of bytes in the string
        Self {
            segments: vec![vec![Bytes::from(s.into_bytes())]],
            total_size,
        }
    }
}
