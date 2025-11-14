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

//! AWS Chunked encoding with trailing checksum support.
//!
//! This module implements the `aws-chunked` content encoding format used by S3
//! for streaming uploads with trailing checksums. The format allows computing
//! checksums incrementally while streaming data, with the checksum value sent
//! as a trailer at the end of the body.
//!
//! # Unsigned Protocol Format (STREAMING-UNSIGNED-PAYLOAD-TRAILER)
//!
//! ```text
//! <hex-chunk-size>\r\n
//! <chunk-data>\r\n
//! ...
//! 0\r\n
//! x-amz-checksum-<algorithm>:<base64-value>\r\n
//! \r\n
//! ```
//!
//! # Signed Protocol Format (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER)
//!
//! ```text
//! <hex-chunk-size>;chunk-signature=<sig>\r\n
//! <chunk-data>\r\n
//! ...
//! 0;chunk-signature=<final-sig>\r\n
//! x-amz-checksum-<algorithm>:<base64-value>\r\n
//! x-amz-trailer-signature:<trailer-sig>\r\n
//! \r\n
//! ```
//!
//! # Wire Format vs Canonical Form
//!
//! **Important**: There are two different line ending conventions:
//!
//! - **Wire format (HTTP protocol)**: Uses `\r\n` (CRLF) per RFC 9112 (HTTP/1.1)
//! - **Canonical form (for signing)**: Uses `\n` (LF) per AWS SigV4 spec
//!
//! When computing the trailer signature, AWS specifies:
//! ```text
//! hash('x-amz-checksum-crc32c:sOO8/Q==\n')  // Note: \n not \r\n
//! ```
//!
//! But the actual bytes sent over HTTP use CRLF line endings.
//!
//! Reference: <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html>

use crate::s3::signer::{ChunkSigningContext, sign_chunk, sign_trailer};
use crate::s3::utils::{ChecksumAlgorithm, b64_encode, sha256_hash};
use bytes::Bytes;
use crc_fast::{CrcAlgorithm, Digest as CrcFastDigest};
use futures_util::Stream;
#[cfg(feature = "ring")]
use ring::digest::{Context, SHA256};
use sha1::{Digest as Sha1Digest, Sha1};
#[cfg(not(feature = "ring"))]
use sha2::Sha256;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

/// Default chunk size for aws-chunked encoding (64 KB).
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Incremental checksum hasher for streaming computation.
enum StreamingHasher {
    Crc32(CrcFastDigest),
    Crc32c(CrcFastDigest),
    Crc64nvme(CrcFastDigest),
    Sha1(Sha1),
    #[cfg(feature = "ring")]
    Sha256(Context),
    #[cfg(not(feature = "ring"))]
    Sha256(Sha256),
}

impl StreamingHasher {
    fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::CRC32 => {
                StreamingHasher::Crc32(CrcFastDigest::new(CrcAlgorithm::Crc32IsoHdlc))
            }
            ChecksumAlgorithm::CRC32C => {
                StreamingHasher::Crc32c(CrcFastDigest::new(CrcAlgorithm::Crc32Iscsi))
            }
            ChecksumAlgorithm::CRC64NVME => {
                StreamingHasher::Crc64nvme(CrcFastDigest::new(CrcAlgorithm::Crc64Nvme))
            }
            ChecksumAlgorithm::SHA1 => StreamingHasher::Sha1(Sha1::new()),
            #[cfg(feature = "ring")]
            ChecksumAlgorithm::SHA256 => StreamingHasher::Sha256(Context::new(&SHA256)),
            #[cfg(not(feature = "ring"))]
            ChecksumAlgorithm::SHA256 => StreamingHasher::Sha256(Sha256::new()),
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            StreamingHasher::Crc32(d) => d.update(data),
            StreamingHasher::Crc32c(d) => d.update(data),
            StreamingHasher::Crc64nvme(d) => d.update(data),
            StreamingHasher::Sha1(h) => h.update(data),
            #[cfg(feature = "ring")]
            StreamingHasher::Sha256(ctx) => ctx.update(data),
            #[cfg(not(feature = "ring"))]
            StreamingHasher::Sha256(h) => h.update(data),
        }
    }

    fn finalize(self) -> String {
        match self {
            // crc-fast returns u64; CRC32 variants need cast to u32
            StreamingHasher::Crc32(d) => b64_encode((d.finalize() as u32).to_be_bytes()),
            StreamingHasher::Crc32c(d) => b64_encode((d.finalize() as u32).to_be_bytes()),
            StreamingHasher::Crc64nvme(d) => b64_encode(d.finalize().to_be_bytes()),
            StreamingHasher::Sha1(h) => {
                let result = h.finalize();
                b64_encode(&result[..])
            }
            #[cfg(feature = "ring")]
            StreamingHasher::Sha256(ctx) => b64_encode(ctx.finish().as_ref()),
            #[cfg(not(feature = "ring"))]
            StreamingHasher::Sha256(h) => {
                let result = h.finalize();
                b64_encode(&result[..])
            }
        }
    }
}

/// State machine for the aws-chunked encoder.
#[derive(Clone, Copy)]
enum EncoderState {
    /// Emitting data chunks
    Streaming,
    /// Emitting the final zero-length chunk marker
    FinalChunk,
    /// Emitting the trailer with checksum
    Trailer,
    /// Done
    Done,
}

/// AWS Chunked encoder that wraps data in aws-chunked format with trailing checksum.
///
/// This encoder takes input data and produces output in the following format:
/// ```text
/// <hex-size>\r\n
/// <data>\r\n
/// 0\r\n
/// x-amz-checksum-<alg>:<base64>\r\n
/// \r\n
/// ```
pub struct AwsChunkedEncoder<S> {
    inner: S,
    algorithm: ChecksumAlgorithm,
    hasher: Option<StreamingHasher>,
    state: EncoderState,
}

impl<S> AwsChunkedEncoder<S> {
    /// Creates a new AWS chunked encoder wrapping the given stream.
    pub fn new(inner: S, algorithm: ChecksumAlgorithm) -> Self {
        Self {
            inner,
            algorithm,
            hasher: Some(StreamingHasher::new(algorithm)),
            state: EncoderState::Streaming,
        }
    }
}

impl<S, E> Stream for AwsChunkedEncoder<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                EncoderState::Streaming => {
                    let inner = Pin::new(&mut self.inner);
                    match inner.poll_next(cx) {
                        Poll::Ready(Some(Ok(chunk))) => {
                            if chunk.is_empty() {
                                continue;
                            }

                            // Update checksum with raw data
                            if let Some(ref mut hasher) = self.hasher {
                                hasher.update(&chunk);
                            }

                            // Format: <hex-size>\r\n<data>\r\n
                            let chunk_header = format!("{:x}\r\n", chunk.len());
                            let mut output =
                                Vec::with_capacity(chunk_header.len() + chunk.len() + 2);
                            output.extend_from_slice(chunk_header.as_bytes());
                            output.extend_from_slice(&chunk);
                            output.extend_from_slice(b"\r\n");

                            return Poll::Ready(Some(Ok(Bytes::from(output))));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // Input stream exhausted, move to final chunk
                            self.state = EncoderState::FinalChunk;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }

                EncoderState::FinalChunk => {
                    // Emit "0\r\n" for the final zero-length chunk
                    self.state = EncoderState::Trailer;
                    return Poll::Ready(Some(Ok(Bytes::from_static(b"0\r\n"))));
                }

                EncoderState::Trailer => {
                    // Compute and emit the trailer
                    let hasher = self.hasher.take().expect("hasher should exist");
                    let checksum_value = hasher.finalize();
                    let trailer = format!(
                        "{}:{}\r\n\r\n",
                        self.algorithm.header_name(),
                        checksum_value
                    );

                    self.state = EncoderState::Done;
                    return Poll::Ready(Some(Ok(Bytes::from(trailer))));
                }

                EncoderState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Calculates the encoded length for aws-chunked format.
///
/// For a given content length and chunk size, returns the total encoded length
/// including all chunk headers, the final zero-length chunk, and the trailer.
pub fn calculate_encoded_length(
    content_length: u64,
    chunk_size: usize,
    algorithm: ChecksumAlgorithm,
) -> u64 {
    let chunk_size = chunk_size as u64;

    // Number of full chunks
    let full_chunks = content_length / chunk_size;
    // Size of the last partial chunk (0 if content divides evenly)
    let last_chunk_size = content_length % chunk_size;
    let has_partial = if last_chunk_size > 0 { 1 } else { 0 };

    // Each chunk: "<hex-size>\r\n<data>\r\n"
    // hex-size length varies based on chunk size
    let hex_len_full = format!("{:x}", chunk_size).len() as u64;
    let hex_len_partial = if last_chunk_size > 0 {
        format!("{:x}", last_chunk_size).len() as u64
    } else {
        0
    };

    // Full chunks overhead: hex_len + 2 (\r\n) + chunk_size + 2 (\r\n)
    let full_chunk_overhead = full_chunks * (hex_len_full + 2 + chunk_size + 2);

    // Partial chunk overhead (if any)
    let partial_chunk_overhead = if has_partial > 0 {
        hex_len_partial + 2 + last_chunk_size + 2
    } else {
        0
    };

    // Final chunk: "0\r\n"
    let final_chunk = 3;

    // Trailer: "x-amz-checksum-<alg>:<base64>\r\n\r\n"
    // Header name length + ":" + base64 checksum length + "\r\n\r\n"
    let trailer_header_len = algorithm.header_name().len() as u64;
    let checksum_b64_len = match algorithm {
        ChecksumAlgorithm::CRC32 | ChecksumAlgorithm::CRC32C => 8, // 4 bytes -> 8 chars base64
        ChecksumAlgorithm::CRC64NVME => 12,                        // 8 bytes -> 12 chars base64
        ChecksumAlgorithm::SHA1 => 28,                             // 20 bytes -> 28 chars base64
        ChecksumAlgorithm::SHA256 => 44,                           // 32 bytes -> 44 chars base64
    };
    let trailer_len = trailer_header_len + 1 + checksum_b64_len + 4; // +1 for ":", +4 for "\r\n\r\n"

    full_chunk_overhead + partial_chunk_overhead + final_chunk + trailer_len
}

/// Returns the default chunk size for aws-chunked encoding.
pub fn default_chunk_size() -> usize {
    DEFAULT_CHUNK_SIZE
}

// ===========================
// Signed AWS Chunked Encoder
// ===========================

/// State machine for the signed aws-chunked encoder.
#[derive(Clone, Copy)]
enum SignedEncoderState {
    /// Emitting signed data chunks
    Streaming,
    /// Emitting the final zero-length chunk with signature
    FinalChunk,
    /// Emitting the checksum trailer header
    Trailer,
    /// Emitting the trailer signature
    TrailerSignature,
    /// Done
    Done,
}

/// AWS Chunked encoder with chunk signing for STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER.
///
/// Each chunk is signed using the AWS Signature V4 chunk signing algorithm.
/// The final trailer is also signed with a trailer signature.
///
/// # Wire Format
///
/// ```text
/// <hex-size>;chunk-signature=<sig>\r\n
/// <data>\r\n
/// 0;chunk-signature=<final-sig>\r\n
/// x-amz-checksum-<alg>:<base64>\r\n
/// x-amz-trailer-signature:<trailer-sig>\r\n
/// \r\n
/// ```
pub struct SignedAwsChunkedEncoder<S> {
    inner: S,
    algorithm: ChecksumAlgorithm,
    hasher: Option<StreamingHasher>,
    state: SignedEncoderState,

    // Signing context
    signing_key: Arc<[u8]>,
    date_time: String,
    scope: String,

    // Signature chain - each chunk's signature becomes the previous for the next
    current_signature: String,

    // Store the checksum value for trailer signature computation
    checksum_value: Option<String>,
}

impl<S> SignedAwsChunkedEncoder<S> {
    /// Creates a new signed AWS chunked encoder wrapping the given stream.
    ///
    /// # Arguments
    /// * `inner` - The underlying data stream
    /// * `algorithm` - The checksum algorithm to use
    /// * `context` - The chunk signing context from request signing
    pub fn new(inner: S, algorithm: ChecksumAlgorithm, context: ChunkSigningContext) -> Self {
        Self {
            inner,
            algorithm,
            hasher: Some(StreamingHasher::new(algorithm)),
            state: SignedEncoderState::Streaming,
            signing_key: context.signing_key,
            date_time: context.date_time,
            scope: context.scope,
            current_signature: context.seed_signature,
            checksum_value: None,
        }
    }

    /// Signs a chunk and returns the signature.
    fn sign_chunk_data(&mut self, chunk_hash: &str) -> String {
        let signature = sign_chunk(
            &self.signing_key,
            &self.date_time,
            &self.scope,
            &self.current_signature,
            chunk_hash,
        );
        self.current_signature = signature.clone();
        signature
    }
}

impl<S, E> Stream for SignedAwsChunkedEncoder<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                SignedEncoderState::Streaming => {
                    let inner = Pin::new(&mut self.inner);
                    match inner.poll_next(cx) {
                        Poll::Ready(Some(Ok(chunk))) => {
                            if chunk.is_empty() {
                                continue;
                            }

                            // Update checksum hasher with raw data
                            if let Some(ref mut hasher) = self.hasher {
                                hasher.update(&chunk);
                            }

                            // Compute SHA256 hash of chunk data for signing
                            let chunk_hash = sha256_hash(&chunk);

                            // Sign the chunk
                            let signature = self.sign_chunk_data(&chunk_hash);

                            // Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n
                            let chunk_header =
                                format!("{:x};chunk-signature={}\r\n", chunk.len(), signature);
                            let mut output =
                                Vec::with_capacity(chunk_header.len() + chunk.len() + 2);
                            output.extend_from_slice(chunk_header.as_bytes());
                            output.extend_from_slice(&chunk);
                            output.extend_from_slice(b"\r\n");

                            return Poll::Ready(Some(Ok(Bytes::from(output))));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // Input stream exhausted, move to final chunk
                            self.state = SignedEncoderState::FinalChunk;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }

                SignedEncoderState::FinalChunk => {
                    // Sign the empty chunk (SHA256 of empty string)
                    const EMPTY_SHA256: &str =
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
                    let signature = self.sign_chunk_data(EMPTY_SHA256);

                    // Emit "0;chunk-signature=<sig>\r\n"
                    let final_chunk = format!("0;chunk-signature={}\r\n", signature);

                    self.state = SignedEncoderState::Trailer;
                    return Poll::Ready(Some(Ok(Bytes::from(final_chunk))));
                }

                SignedEncoderState::Trailer => {
                    // Compute and store the checksum value
                    let hasher = self.hasher.take().expect("hasher should exist");
                    let checksum_value = hasher.finalize();
                    self.checksum_value = Some(checksum_value.clone());

                    // Emit the checksum trailer using CRLF (wire format per RFC 9112)
                    // Note: The canonical form for signing uses LF (\n), but HTTP wire
                    // format uses CRLF (\r\n). See module docs for details.
                    let trailer = format!(
                        "{}:{}\r\n",
                        self.algorithm.header_name().to_lowercase(),
                        checksum_value
                    );

                    self.state = SignedEncoderState::TrailerSignature;
                    return Poll::Ready(Some(Ok(Bytes::from(trailer))));
                }

                SignedEncoderState::TrailerSignature => {
                    // Compute the canonical trailers string for signing.
                    // IMPORTANT: AWS SigV4 canonical form uses LF (\n), NOT CRLF (\r\n).
                    // Per AWS docs: hash('x-amz-checksum-crc32c:sOO8/Q==\n')
                    // This differs from the wire format which uses CRLF.
                    let checksum_value =
                        self.checksum_value.as_ref().expect("checksum should exist");
                    let canonical_trailers = format!(
                        "{}:{}\n", // LF for canonical form (signing)
                        self.algorithm.header_name().to_lowercase(),
                        checksum_value
                    );

                    // Hash the canonical trailers
                    let trailers_hash = sha256_hash(canonical_trailers.as_bytes());

                    // Sign the trailer
                    let trailer_signature = sign_trailer(
                        &self.signing_key,
                        &self.date_time,
                        &self.scope,
                        &self.current_signature,
                        &trailers_hash,
                    );

                    // Emit trailer signature using CRLF (wire format per RFC 9112)
                    // Final \r\n\r\n marks end of trailer section
                    let trailer_sig_line =
                        format!("x-amz-trailer-signature:{}\r\n\r\n", trailer_signature);

                    self.state = SignedEncoderState::Done;
                    return Poll::Ready(Some(Ok(Bytes::from(trailer_sig_line))));
                }

                SignedEncoderState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Calculates the encoded length for signed aws-chunked format.
///
/// For a given content length and chunk size, returns the total encoded length
/// including all chunk headers with signatures, the final zero-length chunk,
/// the checksum trailer, and the trailer signature.
pub fn calculate_signed_encoded_length(
    content_length: u64,
    chunk_size: usize,
    algorithm: ChecksumAlgorithm,
) -> u64 {
    let chunk_size = chunk_size as u64;

    // Number of full chunks
    let full_chunks = content_length / chunk_size;
    // Size of the last partial chunk (0 if content divides evenly)
    let last_chunk_size = content_length % chunk_size;
    let has_partial = if last_chunk_size > 0 { 1 } else { 0 };

    // Each signed chunk: "<hex-size>;chunk-signature=<64-hex>\r\n<data>\r\n"
    // Signature overhead per chunk: ";chunk-signature=" (17) + 64 hex chars = 81 bytes
    let signature_overhead: u64 = 81;

    let hex_len_full = format!("{:x}", chunk_size).len() as u64;
    let hex_len_partial = if last_chunk_size > 0 {
        format!("{:x}", last_chunk_size).len() as u64
    } else {
        0
    };

    // Full chunks: hex_len + signature_overhead + 2 (\r\n) + chunk_size + 2 (\r\n)
    let full_chunk_overhead =
        full_chunks * (hex_len_full + signature_overhead + 2 + chunk_size + 2);

    // Partial chunk (if any)
    let partial_chunk_overhead = if has_partial > 0 {
        hex_len_partial + signature_overhead + 2 + last_chunk_size + 2
    } else {
        0
    };

    // Final chunk: "0;chunk-signature=<64-hex>\r\n" = 1 + 81 + 2 = 84
    let final_chunk = 84;

    // Checksum trailer: "<lowercase-header>:<base64>\r\n"
    // Header name is lowercase (e.g., "x-amz-checksum-crc32")
    let trailer_header_len = algorithm.header_name().to_lowercase().len() as u64;
    let checksum_b64_len = match algorithm {
        ChecksumAlgorithm::CRC32 | ChecksumAlgorithm::CRC32C => 8,
        ChecksumAlgorithm::CRC64NVME => 12,
        ChecksumAlgorithm::SHA1 => 28,
        ChecksumAlgorithm::SHA256 => 44,
    };
    let checksum_trailer = trailer_header_len + 1 + checksum_b64_len + 2; // +1 for ":", +2 for "\r\n"

    // Trailer signature: "x-amz-trailer-signature:<64-hex>\r\n\r\n"
    // = 24 + 64 + 4 = 92 bytes
    let trailer_signature = 92;

    full_chunk_overhead
        + partial_chunk_overhead
        + final_chunk
        + checksum_trailer
        + trailer_signature
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_aws_chunked_encoder_simple() {
        let data = Bytes::from("Hello, World!");
        // Use iter instead of once - iter produces an Unpin stream
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(data.clone())]);

        let mut encoder = AwsChunkedEncoder::new(stream, ChecksumAlgorithm::CRC32);
        let mut output = Vec::new();

        while let Some(chunk) = encoder.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }

        let output_str = String::from_utf8(output).unwrap();

        // Should start with hex size of "Hello, World!" (13 bytes = 'd')
        assert!(output_str.starts_with("d\r\n"));
        // Should contain the data
        assert!(output_str.contains("Hello, World!"));
        // Should end with trailer (header name is mixed-case per S3 spec)
        assert!(output_str.contains("X-Amz-Checksum-CRC32:"));
        assert!(output_str.ends_with("\r\n\r\n"));
        // Should have zero-length final chunk
        assert!(output_str.contains("\r\n0\r\n"));
    }

    #[tokio::test]
    async fn test_aws_chunked_encoder_multiple_chunks() {
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from("Hello, ")),
            Ok(Bytes::from("World!")),
        ];
        let stream = futures_util::stream::iter(chunks);

        let mut encoder = AwsChunkedEncoder::new(stream, ChecksumAlgorithm::CRC64NVME);
        let mut output = Vec::new();

        while let Some(chunk) = encoder.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }

        let output_str = String::from_utf8(output).unwrap();

        // Should have two chunk headers
        assert!(output_str.starts_with("7\r\n")); // "Hello, " is 7 bytes
        assert!(output_str.contains("6\r\n")); // "World!" is 6 bytes
        assert!(output_str.contains("X-Amz-Checksum-CRC64NVME:"));
    }

    #[test]
    fn test_calculate_encoded_length() {
        // Simple case: 100 bytes, 64KB chunks
        let len = calculate_encoded_length(100, 64 * 1024, ChecksumAlgorithm::CRC32);
        // 100 bytes fits in one chunk: "64\r\n" (4) + 100 + "\r\n" (2) + "0\r\n" (3) + trailer
        // "64" is hex for 100, which is "64" (2 chars)
        // trailer: "x-amz-checksum-crc32:" (21) + 8 (base64) + "\r\n\r\n" (4) = 33
        // Total: 2 + 2 + 100 + 2 + 3 + 33 = 142
        assert!(len > 100); // Should be larger than raw content
    }

    // ===========================
    // Signed Encoder Tests
    // ===========================

    fn test_signing_context() -> ChunkSigningContext {
        ChunkSigningContext {
            signing_key: Arc::from(vec![
                // Pre-computed signing key for test credentials
                0x98, 0xf1, 0xd8, 0x89, 0xfe, 0xc4, 0xf4, 0x42, 0x1a, 0xdc, 0x52, 0x2b, 0xab, 0x0c,
                0xe1, 0xf8, 0x2c, 0x6c, 0x4e, 0x4e, 0xc3, 0x9a, 0xe1, 0xf6, 0xcc, 0xf2, 0x0e, 0x8f,
                0x40, 0x89, 0x45, 0x65,
            ]),
            date_time: "20130524T000000Z".to_string(),
            scope: "20130524/us-east-1/s3/aws4_request".to_string(),
            seed_signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
                .to_string(),
        }
    }

    #[tokio::test]
    async fn test_signed_encoder_simple() {
        let data = Bytes::from("Hello, World!");
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(data)]);

        let context = test_signing_context();
        let mut encoder = SignedAwsChunkedEncoder::new(stream, ChecksumAlgorithm::CRC32, context);
        let mut output = Vec::new();

        while let Some(chunk) = encoder.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }

        let output_str = String::from_utf8(output).unwrap();

        // Should start with hex size and chunk-signature
        assert!(output_str.starts_with("d;chunk-signature="));
        // Should contain the data
        assert!(output_str.contains("Hello, World!"));
        // Should have final chunk with signature
        assert!(output_str.contains("0;chunk-signature="));
        // Should have checksum trailer (lowercase)
        assert!(output_str.contains("x-amz-checksum-crc32:"));
        // Should have trailer signature
        assert!(output_str.contains("x-amz-trailer-signature:"));
        // Should end with \r\n\r\n
        assert!(output_str.ends_with("\r\n\r\n"));
    }

    #[tokio::test]
    async fn test_signed_encoder_multiple_chunks() {
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from("Hello, ")),
            Ok(Bytes::from("World!")),
        ];
        let stream = futures_util::stream::iter(chunks);

        let context = test_signing_context();
        let mut encoder = SignedAwsChunkedEncoder::new(stream, ChecksumAlgorithm::CRC32C, context);
        let mut output = Vec::new();

        while let Some(chunk) = encoder.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }

        let output_str = String::from_utf8(output).unwrap();

        // Should have two chunk signatures (different signatures due to chaining)
        let sig_count = output_str.matches(";chunk-signature=").count();
        assert_eq!(sig_count, 3); // 2 data chunks + 1 final chunk

        // Should have checksum trailer
        assert!(output_str.contains("x-amz-checksum-crc32c:"));
        // Should have trailer signature
        assert!(output_str.contains("x-amz-trailer-signature:"));
    }

    #[tokio::test]
    async fn test_signed_encoder_signature_is_64_hex_chars() {
        let data = Bytes::from("test");
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(data)]);

        let context = test_signing_context();
        let mut encoder = SignedAwsChunkedEncoder::new(stream, ChecksumAlgorithm::CRC32, context);
        let mut output = Vec::new();

        while let Some(chunk) = encoder.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }

        let output_str = String::from_utf8(output).unwrap();

        // Extract signatures and verify they're 64 hex chars
        for sig_match in output_str.match_indices(";chunk-signature=") {
            let start = sig_match.0 + sig_match.1.len();
            let sig = &output_str[start..start + 64];
            assert!(
                sig.chars().all(|c| c.is_ascii_hexdigit()),
                "Signature should be hex: {}",
                sig
            );
        }

        // Also check trailer signature
        let trailer_sig_start = output_str.find("x-amz-trailer-signature:").unwrap() + 24;
        let trailer_sig = &output_str[trailer_sig_start..trailer_sig_start + 64];
        assert!(
            trailer_sig.chars().all(|c| c.is_ascii_hexdigit()),
            "Trailer signature should be hex: {}",
            trailer_sig
        );
    }

    #[test]
    fn test_calculate_signed_encoded_length() {
        // 100 bytes, 64KB chunks
        let len = calculate_signed_encoded_length(100, 64 * 1024, ChecksumAlgorithm::CRC32);

        // Should be larger than unsigned (due to signature overhead)
        let unsigned_len = calculate_encoded_length(100, 64 * 1024, ChecksumAlgorithm::CRC32);
        assert!(
            len > unsigned_len,
            "Signed length {} should be > unsigned length {}",
            len,
            unsigned_len
        );
    }

    #[test]
    fn test_calculate_signed_encoded_length_multiple_chunks() {
        // 200KB with 64KB chunks = 3 full chunks + 1 partial + final
        let content_len = 200 * 1024;
        let chunk_size = 64 * 1024;
        let len =
            calculate_signed_encoded_length(content_len, chunk_size, ChecksumAlgorithm::SHA256);

        // Should include all overhead
        assert!(len > content_len);

        // Calculate expected: signature overhead per chunk is 81 bytes
        // Plus final chunk (84), checksum trailer, trailer signature (92)
    }
}
