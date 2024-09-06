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

use std::path::PathBuf;
use std::{ffi::OsString, path::Path, pin::Pin};

use bytes::{Bytes, BytesMut};
use futures_util::Stream;
use rand::prelude::random;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

#[cfg(test)]
use quickcheck::Arbitrary;

type IoResult<T> = Result<T, std::io::Error>;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum Size {
    Known(u64),
    Unknown,
}

impl Size {
    pub fn is_known(&self) -> bool {
        matches!(self, Size::Known(_))
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Size::Unknown)
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Size::Known(v) => Some(*v),
            Size::Unknown => None,
        }
    }
}

impl From<Option<u64>> for Size {
    fn from(value: Option<u64>) -> Self {
        match value {
            Some(v) => Size::Known(v),
            None => Size::Unknown,
        }
    }
}

#[cfg(test)]
impl Arbitrary for Size {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        if bool::arbitrary(g) {
            Size::Known(u64::arbitrary(g))
        } else {
            Size::Unknown
        }
    }
}

/// Object content that can be uploaded or downloaded. Can be constructed from a stream of `Bytes`,
/// a file path, or a `Bytes` object.
pub struct ObjectContent(ObjectContentInner);

enum ObjectContentInner {
    Stream(Pin<Box<dyn Stream<Item = IoResult<Bytes>> + Send>>, Size),
    FilePath(PathBuf),
    Bytes(SegmentedBytes),
}

impl From<Bytes> for ObjectContent {
    fn from(value: Bytes) -> Self {
        ObjectContent(ObjectContentInner::Bytes(SegmentedBytes::from(value)))
    }
}

impl From<String> for ObjectContent {
    fn from(value: String) -> Self {
        ObjectContent(ObjectContentInner::Bytes(SegmentedBytes::from(
            Bytes::from(value),
        )))
    }
}

impl From<Vec<u8>> for ObjectContent {
    fn from(value: Vec<u8>) -> Self {
        ObjectContent(ObjectContentInner::Bytes(SegmentedBytes::from(
            Bytes::from(value),
        )))
    }
}

impl From<&'static [u8]> for ObjectContent {
    fn from(value: &'static [u8]) -> Self {
        ObjectContent(ObjectContentInner::Bytes(SegmentedBytes::from(
            Bytes::from(value),
        )))
    }
}

impl From<&Path> for ObjectContent {
    fn from(value: &Path) -> Self {
        ObjectContent(ObjectContentInner::FilePath(value.to_path_buf()))
    }
}

impl Default for ObjectContent {
    fn default() -> Self {
        ObjectContent(ObjectContentInner::Bytes(SegmentedBytes::new()))
    }
}

impl ObjectContent {
    /// Create a new `ObjectContent` from a stream of `Bytes`.
    pub fn new_from_stream(
        r: impl Stream<Item = IoResult<Bytes>> + Send + 'static,
        size: impl Into<Size>,
    ) -> Self {
        let r = Box::pin(r);
        ObjectContent(ObjectContentInner::Stream(r, size.into()))
    }

    pub async fn to_stream(
        self,
    ) -> IoResult<(Pin<Box<dyn Stream<Item = IoResult<Bytes>> + Send>>, Size)> {
        match self.0 {
            ObjectContentInner::Stream(r, size) => Ok((r, size)),
            ObjectContentInner::FilePath(path) => {
                let file = fs::File::open(&path).await?;
                let size = file.metadata().await?.len();
                let r = tokio_util::io::ReaderStream::new(file);
                Ok((Box::pin(r), Some(size).into()))
            }
            ObjectContentInner::Bytes(sb) => {
                let k = sb.len();
                let r = Box::pin(tokio_stream::iter(sb.into_iter().map(Ok)));
                Ok((r, Some(k as u64).into()))
            }
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) async fn to_content_stream(self) -> IoResult<ContentStream> {
        let (r, size) = self.to_stream().await?;
        Ok(ContentStream::new(r, size))
    }

    /// Load the content into memory and return a `SegmentedBytes` object.
    pub async fn to_segmented_bytes(self) -> IoResult<SegmentedBytes> {
        let mut segmented_bytes = SegmentedBytes::new();
        let (mut r, _) = self.to_stream().await?;
        while let Some(bytes) = r.next().await {
            let bytes = bytes?;
            if bytes.is_empty() {
                break;
            }
            segmented_bytes.append(bytes);
        }
        Ok(segmented_bytes)
    }

    /// Write the content to a file. This function will return the total number
    /// of bytes written to the file. It first writes the content to a temporary
    /// file and then renames the temporary file to the final file path. The
    /// temporary file will be located in the same directory as the final file
    /// path.
    ///
    /// If the file already exists, it will be replaced. If the parent directory
    /// does not exist, an attempt to create it will be made.
    pub async fn to_file(self, file_path: &Path) -> IoResult<u64> {
        if file_path.is_dir() {
            return Err(std::io::Error::other("path is a directory"));
        }
        let parent_dir = file_path.parent().ok_or(std::io::Error::other(format!(
            "path {:?} does not have a parent directory",
            file_path
        )))?;
        if !parent_dir.is_dir() {
            fs::create_dir_all(parent_dir).await?;
        }
        let file_name = file_path.file_name().ok_or(std::io::Error::other(
            "could not get filename component of path",
        ))?;
        let mut tmp_file_name: OsString = file_name.to_os_string();
        tmp_file_name.push(format!("_{}", random::<u64>()));
        let tmp_file_path = parent_dir
            .to_path_buf()
            .join(Path::new(tmp_file_name.as_os_str()));

        let mut total = 0;
        {
            let mut fp = fs::File::open(&tmp_file_path).await?;
            let (mut r, _) = self.to_stream().await?;
            while let Some(bytes) = r.next().await {
                let bytes = bytes?;
                if bytes.is_empty() {
                    break;
                }
                total += bytes.len() as u64;
                fp.write_all(&bytes).await?;
            }
            fp.flush().await?;
        }
        fs::rename(&tmp_file_path, file_path).await?;
        Ok(total)
    }
}

pub(crate) struct ContentStream {
    r: Pin<Box<dyn Stream<Item = IoResult<Bytes>> + Send>>,
    extra: Option<Bytes>,
    size: Size,
}

impl ContentStream {
    pub fn new(
        r: impl Stream<Item = IoResult<Bytes>> + Send + 'static,
        size: impl Into<Size>,
    ) -> Self {
        let r = Box::pin(r);
        Self {
            r,
            extra: None,
            size: size.into(),
        }
    }

    pub fn empty() -> Self {
        Self {
            r: Box::pin(tokio_stream::iter(vec![])),
            extra: None,
            size: Some(0).into(),
        }
    }

    pub fn get_size(&self) -> Size {
        self.size
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
}

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
        SegmentedBytes {
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
                buf.extend_from_slice(bytes);
            }
        }
        buf.freeze()
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
        let segment = &self.sb.segments[self.current_segment];
        if self.current_segment_index >= segment.len() {
            self.current_segment += 1;
            self.current_segment_index = 0;
            return Iterator::next(self);
        }
        let bytes = &segment[self.current_segment_index];
        self.current_segment_index += 1;
        Some(bytes.clone())
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
        let segment = &self.sb.segments[self.current_segment];
        if self.current_segment_index >= segment.len() {
            self.current_segment += 1;
            self.current_segment_index = 0;
            return Iterator::next(self);
        }
        let bytes = &segment[self.current_segment_index];
        self.current_segment_index += 1;
        Some(bytes.clone())
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
        let mut sb = SegmentedBytes::new();
        sb.append(bytes);
        sb
    }
}
