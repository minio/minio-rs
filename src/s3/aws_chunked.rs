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
use bytes::{Bytes, BytesMut};
use crc_fast::{CrcAlgorithm, Digest as CrcFastDigest};
use futures_util::Stream;
#[cfg(feature = "ring")]
use ring::digest::{Context, SHA256, SHA512};
use sha1::{Digest as Sha1Digest, Sha1};
#[cfg(not(feature = "ring"))]
use sha2::{Sha256, Sha512};
use std::collections::VecDeque;
use std::hash::Hasher;
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
    Md5(md5::Context),
    #[cfg(feature = "ring")]
    Sha512(Context),
    #[cfg(not(feature = "ring"))]
    Sha512(Sha512),
    XxHash64(twox_hash::XxHash64),
    XxHash3(twox_hash::XxHash3_64),
    XxHash128(twox_hash::XxHash3_128),
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
            ChecksumAlgorithm::MD5 => StreamingHasher::Md5(md5::Context::new()),
            #[cfg(feature = "ring")]
            ChecksumAlgorithm::SHA512 => StreamingHasher::Sha512(Context::new(&SHA512)),
            #[cfg(not(feature = "ring"))]
            ChecksumAlgorithm::SHA512 => StreamingHasher::Sha512(Sha512::new()),
            ChecksumAlgorithm::XXHash64 => {
                StreamingHasher::XxHash64(twox_hash::XxHash64::with_seed(0))
            }
            ChecksumAlgorithm::XXHash3 => StreamingHasher::XxHash3(twox_hash::XxHash3_64::new()),
            ChecksumAlgorithm::XXHash128 => {
                StreamingHasher::XxHash128(twox_hash::XxHash3_128::new())
            }
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
            StreamingHasher::Md5(ctx) => ctx.consume(data),
            StreamingHasher::Sha512(h) => h.update(data),
            StreamingHasher::XxHash64(h) => h.write(data),
            StreamingHasher::XxHash3(h) => h.write(data),
            StreamingHasher::XxHash128(h) => h.write(data),
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
            StreamingHasher::Md5(ctx) => b64_encode(ctx.finalize().as_slice()),
            #[cfg(feature = "ring")]
            StreamingHasher::Sha512(ctx) => b64_encode(ctx.finish().as_ref()),
            #[cfg(not(feature = "ring"))]
            StreamingHasher::Sha512(h) => {
                let result = h.finalize();
                b64_encode(&result[..])
            }
            StreamingHasher::XxHash64(h) => b64_encode(h.finish().to_be_bytes()),
            StreamingHasher::XxHash3(h) => b64_encode(h.finish().to_be_bytes()),
            StreamingHasher::XxHash128(h) => b64_encode(h.finish_128().to_be_bytes()),
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
    /// The rest of the frame for the chunk just read: its payload and the closing
    /// CRLF. Emitting a frame as separate pieces leaves the payload in the buffer
    /// it arrived in, rather than copying it into a freshly joined one per chunk.
    frame: VecDeque<Bytes>,
}

impl<S> AwsChunkedEncoder<S> {
    /// Creates a new AWS chunked encoder wrapping the given stream.
    pub fn new(inner: S, algorithm: ChecksumAlgorithm) -> Self {
        Self {
            inner,
            algorithm,
            hasher: Some(StreamingHasher::new(algorithm)),
            state: EncoderState::Streaming,
            frame: VecDeque::new(),
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
            // Finish the frame started by an earlier poll before reading on.
            if let Some(piece) = self.frame.pop_front() {
                return Poll::Ready(Some(Ok(piece)));
            }
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
                            let chunk_header = Bytes::from(format!("{:x}\r\n", chunk.len()));
                            self.frame.push_back(chunk);
                            self.frame.push_back(Bytes::from_static(b"\r\n"));

                            return Poll::Ready(Some(Ok(chunk_header)));
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
    let hex_len_full = format!("{chunk_size:x}").len() as u64;
    let hex_len_partial = if last_chunk_size > 0 {
        format!("{last_chunk_size:x}").len() as u64
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
        ChecksumAlgorithm::CRC64NVME | ChecksumAlgorithm::XXHash64 | ChecksumAlgorithm::XXHash3 => {
            12
        } // 8 bytes -> 12 chars base64
        ChecksumAlgorithm::SHA1 => 28,                             // 20 bytes -> 28 chars base64
        ChecksumAlgorithm::SHA256 => 44,                           // 32 bytes -> 44 chars base64
        ChecksumAlgorithm::MD5 | ChecksumAlgorithm::XXHash128 => 24, // 16 bytes -> 24 chars base64
        ChecksumAlgorithm::SHA512 => 88,                           // 64 bytes -> 88 chars base64
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

    /// The rest of the frame for the chunk just read: its payload and the closing
    /// CRLF. The signature covers the payload, which is unchanged by emitting the
    /// frame in pieces instead of joining it into one buffer.
    frame: VecDeque<Bytes>,

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
            frame: VecDeque::new(),
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
            // Finish the frame started by an earlier poll before reading on.
            if let Some(piece) = self.frame.pop_front() {
                return Poll::Ready(Some(Ok(piece)));
            }
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
                            let chunk_header = Bytes::from(format!(
                                "{:x};chunk-signature={signature}\r\n",
                                chunk.len()
                            ));
                            self.frame.push_back(chunk);
                            self.frame.push_back(Bytes::from_static(b"\r\n"));

                            return Poll::Ready(Some(Ok(chunk_header)));
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
        ChecksumAlgorithm::CRC64NVME | ChecksumAlgorithm::XXHash64 | ChecksumAlgorithm::XXHash3 => {
            12
        }
        ChecksumAlgorithm::SHA1 => 28,
        ChecksumAlgorithm::SHA256 => 44,
        ChecksumAlgorithm::MD5 | ChecksumAlgorithm::XXHash128 => 24,
        ChecksumAlgorithm::SHA512 => 88,
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

// ===========================
// Rechunking Stream Wrapper
// ===========================

/// A stream wrapper that re-chunks incoming data to a fixed chunk size.
///
/// This ensures that the actual chunks produced match the chunk size assumed
/// by `calculate_encoded_length` and `calculate_signed_encoded_length`,
/// preventing Content-Length mismatches when the input stream produces
/// differently-sized chunks.
///
/// Payload bytes are not copied when the input already holds at least a full
/// chunk: the chunk is sliced out of the incoming `Bytes`, which shares the
/// buffer by reference count. Only an input piece too small to emit on its own
/// is accumulated, and then at most `chunk_size` bytes are copied per boundary.
/// Re-buffering the whole body instead would make a large upload quadratic,
/// because emitting each chunk would shift the remaining tail down.
pub struct RechunkingStream<S> {
    inner: S,
    chunk_size: usize,
    /// The input piece being consumed, sliced from the front as chunks are emitted.
    pending: Bytes,
    /// Holds input pieces shorter than `chunk_size` until a full chunk can be emitted.
    partial: BytesMut,
    done: bool,
}

impl<S> RechunkingStream<S> {
    /// Creates a new rechunking stream wrapper.
    ///
    /// The wrapper buffers incoming data and emits chunks of exactly `chunk_size` bytes,
    /// except for the final chunk which may be smaller.
    ///
    /// # Panics
    ///
    /// Panics when `chunk_size` is zero, because a stream of zero-byte chunks
    /// never makes progress.
    pub fn new(inner: S, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk size must be non-zero");
        Self {
            inner,
            chunk_size,
            pending: Bytes::new(),
            partial: BytesMut::new(),
            done: false,
        }
    }

    /// Creates a new rechunking stream with the default chunk size (64 KB).
    pub fn with_default_chunk_size(inner: S) -> Self {
        Self::new(inner, DEFAULT_CHUNK_SIZE)
    }
}

impl<S, E> Stream for RechunkingStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let chunk_size = self.chunk_size;

        loop {
            // A whole chunk is already accumulated. `BytesMut::split_to` hands over
            // the front and keeps the remainder without moving either.
            if self.partial.len() >= chunk_size {
                return Poll::Ready(Some(Ok(self.partial.split_to(chunk_size).freeze())));
            }

            // Nothing accumulated and the input piece covers a whole chunk, so serve
            // it by reference. This is the path a large upload takes for every chunk.
            if self.partial.is_empty() && self.pending.len() >= chunk_size {
                return Poll::Ready(Some(Ok(self.pending.split_to(chunk_size))));
            }

            if self.done {
                if !self.partial.is_empty() {
                    return Poll::Ready(Some(Ok(std::mem::take(&mut self.partial).freeze())));
                }
                if !self.pending.is_empty() {
                    return Poll::Ready(Some(Ok(std::mem::take(&mut self.pending))));
                }
                return Poll::Ready(None);
            }

            // Top the accumulator up to exactly one chunk and no further, so that the
            // remainder of a large input piece can still be served by reference.
            if !self.pending.is_empty() {
                if self.partial.is_empty() {
                    // One chunk is all the accumulator ever holds, so reserving it
                    // once keeps a run of small input pieces from regrowing it.
                    self.partial.reserve(chunk_size);
                }
                let take = (chunk_size - self.partial.len()).min(self.pending.len());
                let head = self.pending.split_to(take);
                self.partial.extend_from_slice(&head);
                continue;
            }

            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    self.pending = chunk;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => self.done = true,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
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

    // ===========================
    // RechunkingStream Tests
    // ===========================

    #[tokio::test]
    async fn test_rechunking_stream_combines_small_chunks() {
        // Create many small 1KB chunks
        let chunk_size = 1024;
        let num_chunks = 10;
        let chunks: Vec<Result<Bytes, std::io::Error>> = (0..num_chunks)
            .map(|i| Ok(Bytes::from(vec![i as u8; chunk_size])))
            .collect();

        let stream = futures_util::stream::iter(chunks);
        let mut rechunker = RechunkingStream::new(stream, 4096); // 4KB target

        let mut output_chunks = Vec::new();
        while let Some(chunk) = rechunker.next().await {
            output_chunks.push(chunk.unwrap());
        }

        // 10 x 1KB = 10KB, rechunked to 4KB = 2 full + 1 partial (2KB)
        assert_eq!(output_chunks.len(), 3);
        assert_eq!(output_chunks[0].len(), 4096);
        assert_eq!(output_chunks[1].len(), 4096);
        assert_eq!(output_chunks[2].len(), 2048);

        // Total bytes preserved
        let total: usize = output_chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, num_chunks * chunk_size);
    }

    #[tokio::test]
    async fn test_rechunking_stream_passes_large_chunks() {
        // Single chunk larger than target size
        let data = Bytes::from(vec![42u8; 10000]);
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(data)]);

        let mut rechunker = RechunkingStream::new(stream, 4096);

        let mut output_chunks = Vec::new();
        while let Some(chunk) = rechunker.next().await {
            output_chunks.push(chunk.unwrap());
        }

        // 10000 bytes / 4096 = 2 full + 1 partial (1808)
        assert_eq!(output_chunks.len(), 3);
        assert_eq!(output_chunks[0].len(), 4096);
        assert_eq!(output_chunks[1].len(), 4096);
        assert_eq!(output_chunks[2].len(), 1808);
    }

    #[tokio::test]
    async fn test_rechunking_stream_exact_multiple() {
        // Data that divides evenly into chunk size
        let data = Bytes::from(vec![1u8; 8192]);
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(data)]);

        let mut rechunker = RechunkingStream::new(stream, 4096);

        let mut output_chunks = Vec::new();
        while let Some(chunk) = rechunker.next().await {
            output_chunks.push(chunk.unwrap());
        }

        assert_eq!(output_chunks.len(), 2);
        assert_eq!(output_chunks[0].len(), 4096);
        assert_eq!(output_chunks[1].len(), 4096);
    }

    #[tokio::test]
    async fn test_rechunking_stream_empty() {
        let stream = futures_util::stream::iter(Vec::<Result<Bytes, std::io::Error>>::new());
        let mut rechunker = RechunkingStream::new(stream, 4096);

        let result = rechunker.next().await;
        assert!(result.is_none());
    }

    /// A zero chunk size has to be rejected at construction. Every poll would
    /// find a whole chunk accumulated, so the stream would emit empty chunks
    /// forever and never read its input.
    #[test]
    #[should_panic(expected = "chunk size must be non-zero")]
    fn rechunking_stream_rejects_a_zero_chunk_size() {
        let stream = futures_util::stream::iter(Vec::<Result<Bytes, std::io::Error>>::new());
        let _ = RechunkingStream::new(stream, 0);
    }

    /// An error from the input stream reaches the caller as the very next item,
    /// even when a piece too short to emit is already buffered. Flushing that
    /// piece ahead of the error would hand the consumer a chunk boundary the
    /// input never had, and a caller that stops at the error would read it as a
    /// complete body.
    #[tokio::test]
    async fn rechunker_reports_an_input_error_before_the_buffered_piece() {
        const CHUNK: usize = 4096;
        let items = vec![
            Ok(Bytes::from_static(b"a piece shorter than one chunk")),
            Err(std::io::Error::other("inner stream failed")),
        ];
        let mut rechunker = RechunkingStream::new(futures_util::stream::iter(items), CHUNK);

        match rechunker.next().await {
            Some(Err(e)) => assert_eq!(e.to_string(), "inner stream failed"),
            Some(Ok(chunk)) => panic!("emitted {} buffered bytes before the error", chunk.len()),
            None => panic!("ended without reporting the error"),
        }
    }

    #[tokio::test]
    async fn test_rechunking_stream_preserves_data() {
        // Verify data integrity through rechunking
        let original: Vec<u8> = (0..=255).cycle().take(15000).collect();
        let chunks: Vec<Result<Bytes, std::io::Error>> = original
            .chunks(100) // 100-byte input chunks
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();

        let stream = futures_util::stream::iter(chunks);
        let mut rechunker = RechunkingStream::new(stream, 4096);

        let mut output = Vec::new();
        while let Some(chunk) = rechunker.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(output, original);
    }

    /// The encoder declares `Content-Length` up front from
    /// `calculate_encoded_length`, so the stream it then produces must be exactly
    /// that long. A rechunker that emitted different boundaries would desynchronize
    /// the two and every upload would fail on the wire, not in these tests.
    #[tokio::test]
    async fn encoded_stream_length_matches_the_declared_content_length() {
        const CHUNK: usize = 4096;
        // Bodies that divide evenly, leave a remainder, fall short of one chunk,
        // and land exactly on a boundary.
        for body_len in [
            0usize,
            1,
            100,
            CHUNK - 1,
            CHUNK,
            CHUNK + 1,
            3 * CHUNK,
            10_000,
        ] {
            // The same body delivered whole, and split into pieces that straddle
            // chunk boundaries, must encode to the same length either way.
            for piece in [body_len.max(1), 1000, 7] {
                let body = vec![0xA5u8; body_len];
                let segments: Vec<Result<Bytes, std::io::Error>> = body
                    .chunks(piece)
                    .map(|c| Ok(Bytes::copy_from_slice(c)))
                    .collect();
                let stream = futures_util::stream::iter(segments);
                let rechunked = RechunkingStream::new(stream, CHUNK);
                let mut encoder = AwsChunkedEncoder::new(rechunked, ChecksumAlgorithm::CRC32C);

                let mut encoded = 0u64;
                while let Some(piece) = encoder.next().await {
                    encoded += piece.unwrap().len() as u64;
                }

                let declared =
                    calculate_encoded_length(body_len as u64, CHUNK, ChecksumAlgorithm::CRC32C);
                assert_eq!(
                    encoded, declared,
                    "body {body_len} in {piece}-byte pieces: encoded {encoded}, declared {declared}"
                );
            }
        }
    }

    /// Whatever the incoming segmentation, the rechunker must emit full chunks
    /// followed by at most one short tail, and hand back the original bytes in
    /// order. Slicing the input by reference must not reorder or drop any of it.
    #[tokio::test]
    async fn rechunker_preserves_bytes_and_boundaries_for_any_segmentation() {
        const CHUNK: usize = 256;
        let body: Vec<u8> = (0..2000u32).map(|i| (i % 251) as u8).collect();

        for piece in [1usize, 3, 255, 256, 257, 1000, 2000] {
            let segments: Vec<Result<Bytes, std::io::Error>> = body
                .chunks(piece)
                .map(|c| Ok(Bytes::copy_from_slice(c)))
                .collect();
            let mut rechunker = RechunkingStream::new(futures_util::stream::iter(segments), CHUNK);

            let mut out = Vec::new();
            let mut sizes = Vec::new();
            while let Some(c) = rechunker.next().await {
                let c = c.unwrap();
                sizes.push(c.len());
                out.extend_from_slice(&c);
            }

            assert_eq!(out, body, "bytes differ for {piece}-byte input pieces");
            let (last, full) = sizes.split_last().expect("a non-empty body yields a chunk");
            assert!(
                full.iter().all(|&n| n == CHUNK),
                "non-final chunk was short for {piece}-byte input pieces: {sizes:?}"
            );
            assert!(
                *last > 0 && *last <= CHUNK,
                "final chunk out of range for {piece}-byte input pieces: {sizes:?}"
            );
        }
    }
}
