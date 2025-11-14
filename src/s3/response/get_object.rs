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
use crate::s3::builders::ObjectContent;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response_traits::{
    HasBucket, HasChecksumHeaders, HasEtagFromHeaders, HasObject, HasRegion, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{ChecksumAlgorithm, b64_encode, compute_checksum};
use async_trait::async_trait;
use bytes::Bytes;
use crc_fast::{CrcAlgorithm, Digest as CrcFastDigest};
use futures_util::{Stream, TryStreamExt};
use http::HeaderMap;
#[cfg(feature = "ring")]
use ring::digest::{Context, SHA256};
use sha1::{Digest as Sha1Digest, Sha1};
#[cfg(not(feature = "ring"))]
use sha2::Sha256;
use std::io;
use std::mem;
use std::pin::Pin;

/// Type alias for a boxed byte stream with size, used by [`GetObjectResponse::into_boxed_stream`].
pub type BoxedByteStream = (
    Pin<Box<dyn futures_util::Stream<Item = std::io::Result<Bytes>> + Send>>,
    u64,
);
use std::task::{Context as TaskContext, Poll};

/// Stateful checksum hasher for streaming verification.
///
/// This enum provides incremental checksum computation across multiple data chunks,
/// enabling efficient verification of large objects without loading them entirely into memory.
/// Each variant wraps the appropriate hasher implementation for its algorithm.
///
/// The hasher is used internally by [`GetObjectResponse::content()`] to verify checksums
/// transparently during streaming, with minimal performance overhead.
enum ChecksumHasher {
    Crc32(CrcFastDigest),
    Crc32c(CrcFastDigest),
    Crc64nvme(CrcFastDigest),
    Sha1(Sha1),
    #[cfg(feature = "ring")]
    Sha256(Context),
    #[cfg(not(feature = "ring"))]
    Sha256(Sha256),
}

impl ChecksumHasher {
    /// Creates a new checksum hasher for the specified algorithm.
    ///
    /// Initializes the appropriate hasher implementation with cached instances
    /// for CRC variants to optimize performance.
    ///
    /// # Arguments
    ///
    /// * `algorithm` - The checksum algorithm to use for verification
    fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::CRC32 => {
                ChecksumHasher::Crc32(CrcFastDigest::new(CrcAlgorithm::Crc32IsoHdlc))
            }
            ChecksumAlgorithm::CRC32C => {
                ChecksumHasher::Crc32c(CrcFastDigest::new(CrcAlgorithm::Crc32Iscsi))
            }
            ChecksumAlgorithm::CRC64NVME => {
                ChecksumHasher::Crc64nvme(CrcFastDigest::new(CrcAlgorithm::Crc64Nvme))
            }
            ChecksumAlgorithm::SHA1 => ChecksumHasher::Sha1(Sha1::new()),
            #[cfg(feature = "ring")]
            ChecksumAlgorithm::SHA256 => ChecksumHasher::Sha256(Context::new(&SHA256)),
            #[cfg(not(feature = "ring"))]
            ChecksumAlgorithm::SHA256 => ChecksumHasher::Sha256(Sha256::new()),
        }
    }

    /// Updates the checksum computation with a new chunk of data.
    ///
    /// This method is called incrementally as data streams through, allowing
    /// verification without buffering the entire object in memory.
    ///
    /// # Arguments
    ///
    /// * `data` - The next chunk of data to include in the checksum
    fn update(&mut self, data: &[u8]) {
        match self {
            ChecksumHasher::Crc32(digest) => digest.update(data),
            ChecksumHasher::Crc32c(digest) => digest.update(data),
            ChecksumHasher::Crc64nvme(digest) => digest.update(data),
            ChecksumHasher::Sha1(hasher) => hasher.update(data),
            #[cfg(feature = "ring")]
            ChecksumHasher::Sha256(ctx) => ctx.update(data),
            #[cfg(not(feature = "ring"))]
            ChecksumHasher::Sha256(hasher) => hasher.update(data),
        }
    }

    /// Completes the checksum computation and returns the base64-encoded result.
    ///
    /// This consumes the hasher and produces the final checksum value in the format
    /// expected by S3 headers (base64-encoded). The result can be compared directly
    /// with the checksum value from response headers.
    ///
    /// # Returns
    ///
    /// Base64-encoded checksum string matching the S3 header format.
    fn finalize(self) -> String {
        match self {
            // crc-fast returns u64 for all algorithms; CRC32 variants need cast to u32
            ChecksumHasher::Crc32(digest) => b64_encode((digest.finalize() as u32).to_be_bytes()),
            ChecksumHasher::Crc32c(digest) => b64_encode((digest.finalize() as u32).to_be_bytes()),
            ChecksumHasher::Crc64nvme(digest) => b64_encode(digest.finalize().to_be_bytes()),
            ChecksumHasher::Sha1(hasher) => {
                let result = hasher.finalize();
                b64_encode(&result[..])
            }
            #[cfg(feature = "ring")]
            ChecksumHasher::Sha256(ctx) => b64_encode(ctx.finish().as_ref()),
            #[cfg(not(feature = "ring"))]
            ChecksumHasher::Sha256(hasher) => {
                let result = hasher.finalize();
                b64_encode(&result[..])
            }
        }
    }
}

/// A stream wrapper that computes checksum incrementally while streaming data
struct ChecksumVerifyingStream<S> {
    inner: S,
    hasher: Option<ChecksumHasher>,
    expected_checksum: String,
    finished: bool,
}

impl<S> ChecksumVerifyingStream<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    fn new(stream: S, algorithm: ChecksumAlgorithm, expected_checksum: String) -> Self {
        Self {
            inner: stream,
            hasher: Some(ChecksumHasher::new(algorithm)),
            expected_checksum,
            finished: false,
        }
    }
}

impl<S> Stream for ChecksumVerifyingStream<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                if let Some(hasher) = &mut self.hasher {
                    hasher.update(&bytes);
                }
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.finished = true;
                Poll::Ready(Some(Err(io::Error::other(e))))
            }
            Poll::Ready(None) => {
                self.finished = true;
                if let Some(hasher) = self.hasher.take() {
                    let computed = hasher.finalize();
                    if computed != self.expected_checksum {
                        return Poll::Ready(Some(Err(io::Error::other(format!(
                            "Checksum mismatch: expected {}, computed {}",
                            self.expected_checksum, computed
                        )))));
                    }
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct GetObjectResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes, // Note: not used
    resp: reqwest::Response,
    verify_checksum: bool,
}

impl_has_s3fields!(GetObjectResponse);

impl HasBucket for GetObjectResponse {}
impl HasRegion for GetObjectResponse {}
impl HasObject for GetObjectResponse {}
impl HasVersion for GetObjectResponse {}
impl HasEtagFromHeaders for GetObjectResponse {}
impl HasChecksumHeaders for GetObjectResponse {}

impl GetObjectResponse {
    /// Checks if the checksum is a composite (multipart) checksum.
    ///
    /// Composite checksums are returned for objects uploaded via multipart upload.
    /// They represent a checksum-of-checksums and cannot be verified by computing
    /// a checksum over the full object data.
    ///
    /// Detection is based solely on the `x-amz-checksum-type: COMPOSITE` header.
    /// We intentionally do NOT try to detect composite checksums by parsing the
    /// checksum value for a `-N` suffix, as this could cause false positives if
    /// the server uses base64url encoding (which includes `-` in its alphabet).
    fn is_composite_checksum(&self) -> bool {
        if let Some(checksum_type) = self.checksum_type()
            && checksum_type.eq_ignore_ascii_case("COMPOSITE")
        {
            return true;
        }
        false
    }

    /// Returns the content of the object as a (streaming) byte buffer. Note: consumes the response.
    ///
    /// If `verify_checksum` is enabled and the server provided checksums, the stream will
    /// automatically verify the checksum incrementally as data is read, maintaining streaming performance.
    ///
    /// **Note on multipart objects**: Objects uploaded via multipart upload have COMPOSITE checksums
    /// (checksum-of-checksums) which cannot be verified by computing a checksum over the downloaded
    /// data. For these objects, checksum verification is automatically skipped.
    pub fn content(self) -> Result<ObjectContent, Error> {
        let content_length: u64 = self.object_size()?;

        // Skip verification for composite checksums (multipart uploads)
        // Composite checksums are checksum-of-checksums and cannot be verified
        // by computing a checksum over the full object data
        if self.is_composite_checksum() {
            log::debug!(
                "Skipping checksum verification for composite checksum (multipart upload). \
                 Composite checksums cannot be verified without part boundaries."
            );
            let body = self.resp.bytes_stream().map_err(std::io::Error::other);
            return Ok(ObjectContent::new_from_stream(body, Some(content_length)));
        }

        if let (true, Some(algorithm)) = (self.verify_checksum, self.detect_checksum_algorithm())
            && let Some(expected) = self.get_checksum(algorithm)
        {
            let stream = self.resp.bytes_stream();
            let verifying_stream = ChecksumVerifyingStream::new(stream, algorithm, expected);
            return Ok(ObjectContent::new_from_stream(
                verifying_stream,
                Some(content_length),
            ));
        }

        let body = self.resp.bytes_stream().map_err(std::io::Error::other);
        Ok(ObjectContent::new_from_stream(body, Some(content_length)))
    }

    /// Returns the content as a boxed stream for direct streaming access.
    ///
    /// This is more efficient than `content().to_stream().await` for scenarios
    /// requiring minimal overhead, as it bypasses the async wrapper entirely.
    /// Use this for high-throughput scenarios like DataFusion queries.
    pub fn into_boxed_stream(self) -> Result<BoxedByteStream, Error> {
        let content_length = self.object_size()?;
        let stream = Box::pin(self.resp.bytes_stream().map_err(std::io::Error::other));
        Ok((stream, content_length))
    }

    /// Consumes the response and returns all content as bytes.
    ///
    /// **Memory usage**: This loads the entire object into memory. For objects
    /// larger than available RAM, this may cause out-of-memory errors. For large
    /// objects, use [`into_boxed_stream`](Self::into_boxed_stream) to process
    /// data incrementally.
    pub async fn into_bytes(self) -> Result<Bytes, Error> {
        self.resp
            .bytes()
            .await
            .map_err(|e| ValidationErr::HttpError(e).into())
    }

    /// Sets whether to automatically verify checksums when calling `content()`.
    /// Default is `true`. Verification is performed incrementally during streaming with minimal overhead.
    /// Set to `false` to disable checksum verification entirely.
    pub fn with_verification(mut self, verify: bool) -> Self {
        self.verify_checksum = verify;
        self
    }

    /// Returns the content size (in Bytes) of the object.
    pub fn object_size(&self) -> Result<u64, ValidationErr> {
        self.resp
            .content_length()
            .ok_or(ValidationErr::ContentLengthUnknown)
    }

    /// Returns the content with automatic checksum verification.
    ///
    /// Downloads the full content, computes its checksum, and verifies against server checksum.
    ///
    /// **Note on multipart objects**: Objects uploaded via multipart upload have COMPOSITE checksums
    /// (checksum-of-checksums) which cannot be verified by computing a checksum over the downloaded
    /// data. For these objects, checksum verification is automatically skipped and the content is
    /// returned without verification.
    pub async fn content_verified(self) -> Result<Bytes, Error> {
        // Skip verification for composite checksums (multipart uploads)
        if self.is_composite_checksum() {
            log::debug!(
                "Skipping checksum verification for composite checksum (multipart upload). \
                 Composite checksums cannot be verified without part boundaries."
            );
            return self
                .resp
                .bytes()
                .await
                .map_err(|e| ValidationErr::HttpError(e).into());
        }

        let algorithm = self.detect_checksum_algorithm();
        let expected_checksum = algorithm.and_then(|algo| self.get_checksum(algo));

        let bytes = self.resp.bytes().await.map_err(ValidationErr::HttpError)?;

        if let (Some(algo), Some(expected)) = (algorithm, expected_checksum) {
            let computed = compute_checksum(algo, &bytes);

            if computed != expected {
                return Err(Error::Validation(ValidationErr::ChecksumMismatch {
                    expected,
                    computed,
                }));
            }
        }

        Ok(bytes)
    }

    /// Returns whether the object has a composite checksum (from multipart upload).
    ///
    /// This can be used to check if checksum verification will be skipped.
    pub fn has_composite_checksum(&self) -> bool {
        self.is_composite_checksum()
    }
}

#[async_trait]
impl FromS3Response for GetObjectResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        Ok(Self {
            request,
            headers: mem::take(resp.headers_mut()),
            body: Bytes::new(),
            resp,
            verify_checksum: true, // Default to auto-verify
        })
    }
}
