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

use crate::s3::segmented_bytes::SegmentedBytes;
use async_std::io::{ReadExt, WriteExt};
use bytes::Bytes;
use futures_util::stream::{self, Stream, StreamExt};
use std::path::PathBuf;
use std::{fs, path::Path, pin::Pin};
use uuid::Uuid;

#[cfg(test)]
use quickcheck::Arbitrary;

type IoResult<T> = core::result::Result<T, std::io::Error>;

// region: Size

#[derive(Debug, Clone, PartialEq, Eq, Copy, Default)]
pub enum Size {
    Known(u64),
    #[default]
    Unknown,
}

impl Size {
    /// Returns `true` if the size is known and `false` otherwise.
    pub fn is_known(&self) -> bool {
        matches!(self, Size::Known(_))
    }

    /// Returns `true` if the size is unknown and `false` otherwise.
    pub fn is_unknown(&self) -> bool {
        matches!(self, Size::Unknown)
    }

    /// Returns the size if known, otherwise returns `None`.
    pub fn value(&self) -> Option<u64> {
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

impl From<u64> for Size {
    fn from(value: u64) -> Self {
        Size::Known(value)
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
// endregion: Size

/// Object content that can be uploaded or downloaded.
///
/// Can be constructed from a stream of `Bytes`, a file path, or a `Bytes` object.
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

impl From<&'static str> for ObjectContent {
    fn from(value: &'static str) -> Self {
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
                let mut file = async_std::fs::File::open(&path).await?;
                let metadata = file.metadata().await?;
                let size = metadata.len();

                // Define a stream that reads the file in chunks
                let stream = async_stream::try_stream! {
                    let mut buf = vec![0u8; 8192];
                    loop {
                        let n = file.read(&mut buf).await?;
                        if n == 0 {
                            break;
                        }
                        yield Bytes::copy_from_slice(&buf[..n]);
                    }
                };

                Ok((Box::pin(stream), Some(size).into()))
            }

            ObjectContentInner::Bytes(sb) => {
                let k = sb.len();
                let r = Box::pin(stream::iter(sb.into_iter().map(Ok)));
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
            "path {file_path:?} does not have a parent directory"
        )))?;
        if !parent_dir.is_dir() {
            async_std::fs::create_dir_all(parent_dir).await?;
        }
        let file_name = file_path.file_name().ok_or(std::io::Error::other(
            "could not get filename-component of path",
        ))?;
        let mut tmp_file_name = file_name.to_os_string();
        tmp_file_name.push(format!("_{}", Uuid::new_v4().to_string().replace('-', "_")));
        let tmp_file_path = parent_dir.join(tmp_file_name);

        let mut total_bytes_written = 0;
        let mut fp = async_std::fs::OpenOptions::new()
            .write(true)
            .create(true) // Ensures that the file will be created if it does not already exist
            .truncate(true) // Clears the contents (truncates the file size to 0) before writing
            .open(&tmp_file_path)
            .await?;
        let (mut r, _) = self.to_stream().await?;
        while let Some(bytes) = r.next().await {
            let bytes = bytes?;
            if bytes.is_empty() {
                break;
            }
            total_bytes_written += bytes.len() as u64;
            fp.write_all(&bytes).await?;
        }
        fp.flush().await?;
        fs::rename(&tmp_file_path, file_path)?;
        Ok(total_bytes_written)
    }
}

pub struct ContentStream {
    r: Pin<Box<dyn Stream<Item = IoResult<Bytes>> + Send>>,
    extra: Option<Bytes>,
    size: Size,
}

impl Default for ContentStream {
    fn default() -> Self {
        ContentStream::empty()
    }
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
            r: Box::pin(stream::iter(vec![])),
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
