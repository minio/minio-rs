//! Response traits for accessing S3 metadata from HTTP response headers.
//!
//! This module provides a collection of traits that enable typed, ergonomic access to
//! metadata from S3 API responses. These traits extract data from HTTP headers and response
//! bodies returned by various S3 operations.
//!
//! # Design Philosophy
//!
//! Rather than exposing raw headers directly, these traits provide:
//! - **Type-safe access**: Automatic parsing and type conversion
//! - **Consistent API**: Uniform method names across different response types
//! - **Composability**: Mix and match traits based on what metadata is available
//!
//! # Metadata Sources
//!
//! Metadata is available from two primary sources:
//!
//! ## 1. HEAD Requests (Metadata Only)
//!
//! Operations like [`stat_object`](crate::s3::client::MinioClient::stat_object) use HEAD requests
//! to retrieve object metadata without downloading the object body. These responses typically
//! implement traits like:
//! - [`HasVersion`]: Object version ID (via `x-amz-version-id` header)
//! - [`HasObjectSize`]: Object size in bytes (via `x-amz-object-size` or `Content-Length` header)
//! - [`HasEtagFromHeaders`]: Object ETag/hash (via `ETag` header)
//! - [`HasChecksumHeaders`]: Object checksum values (via `x-amz-checksum-*` headers)
//! - [`HasIsDeleteMarker`]: Whether the object is a delete marker (via `x-amz-delete-marker` header)
//!
//! ## 2. GET Requests (Metadata + Body)
//!
//! Operations like [`get_object`](crate::s3::client::MinioClient::get_object) return both
//! metadata headers AND the object body. These responses can implement both header-based
//! traits (above) and body-parsing traits like:
//! - [`HasEtagFromBody`]: ETag parsed from XML response body
//!
//! # Example: StatObjectResponse
//!
//! The [`StatObjectResponse`](crate::s3::response::StatObjectResponse) demonstrates how
//! multiple traits compose together. It uses a HEAD request and provides:
//!
//! ```rust,ignore
//! impl HasBucket for StatObjectResponse {}
//! impl HasRegion for StatObjectResponse {}
//! impl HasObject for StatObjectResponse {}
//! impl HasEtagFromHeaders for StatObjectResponse {}
//! impl HasIsDeleteMarker for StatObjectResponse {}
//! impl HasChecksumHeaders for StatObjectResponse {}
//! impl HasVersion for StatObjectResponse {}       // Version ID from header
//! impl HasObjectSize for StatObjectResponse {}    // Size from header
//! ```
//!
//! This allows users to access metadata uniformly:
//!
//! ```rust,ignore
//! let response = client.stat_object(&args).await?;
//! let size = response.object_size();           // From HasObjectSize trait
//! let version = response.version_id();          // From HasVersion trait
//! let checksum = response.checksum_crc32c()?;  // From HasChecksumHeaders trait
//! ```
//!
//! # Performance Considerations
//!
//! - **HEAD vs GET**: HEAD requests are faster when you only need metadata (no body transfer)
//! - **Header parsing**: Trait methods use `#[inline]` for zero-cost abstractions
//! - **Lazy evaluation**: Metadata is parsed on-demand, not upfront

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::*;
use crate::s3::types::S3Request;
use crate::s3::utils::{ChecksumAlgorithm, get_text_result, parse_bool, trim_quotes};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::collections::HashMap;
use xmltree::Element;

#[macro_export]
/// Implements the `FromS3Response` trait for the specified types.
macro_rules! impl_from_s3response {
    ($($ty:ty),* $(,)?) => {
        $(
            #[async_trait::async_trait]
            impl $crate::s3::types::FromS3Response for $ty {
                async fn from_s3response(
                    request: $crate::s3::types::S3Request,
                    response: Result<reqwest::Response, $crate::s3::error::Error>,
                ) -> Result<Self, $crate::s3::error::Error> {
                    let mut resp: reqwest::Response = response?;
                    Ok(Self {
                        request,
                        headers: std::mem::take(resp.headers_mut()),
                        body: resp.bytes().await.map_err($crate::s3::error::ValidationErr::from)?,
                    })
                }
            }
        )*
    };
}

#[macro_export]
/// Implements the `FromS3Response` trait for the specified types with an additional `object_size` field.
macro_rules! impl_from_s3response_with_size {
    ($($ty:ty),* $(,)?) => {
        $(
            #[async_trait::async_trait]
            impl $crate::s3::types::FromS3Response for $ty {
                async fn from_s3response(
                    request: $crate::s3::types::S3Request,
                    response: Result<reqwest::Response, $crate::s3::error::Error>,
                ) -> Result<Self, $crate::s3::error::Error> {
                    let mut resp: reqwest::Response = response?;
                    Ok(Self {
                        request,
                        headers: std::mem::take(resp.headers_mut()),
                        body: resp.bytes().await.map_err($crate::s3::error::ValidationErr::from)?,
                        object_size: 0, // Default value, can be set later
                    })
                }
            }
        )*
    };
}

#[macro_export]
/// Implements the `HasS3Fields` trait for the specified types.
macro_rules! impl_has_s3fields {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::s3::response_traits::HasS3Fields for $ty {
                /// The request that was sent to the S3 API.
                #[inline]
                fn request(&self) -> &$crate::s3::types::S3Request {
                    &self.request
                }

                /// The response of the S3 API.
                #[inline]
                fn headers(&self) -> &http::HeaderMap {
                    &self.headers
                }

                /// The response of the S3 API.
                #[inline]
                fn body(&self) -> &bytes::Bytes {
                    &self.body
                }
            }
        )*
    };
}

pub trait HasS3Fields {
    /// The request that was sent to the S3 API.
    fn request(&self) -> &S3Request;
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    fn headers(&self) -> &HeaderMap;
    /// The response body returned by the server, which may contain the object data or other information.
    fn body(&self) -> &Bytes;
}
/// Returns the name of the S3 bucket.
pub trait HasBucket: HasS3Fields {
    /// Returns the name of the S3 bucket.
    #[inline]
    fn bucket(&self) -> &str {
        //TODO consider using BucketName type
        self.request()
            .bucket
            .as_ref()
            .map(|b| b.as_str())
            .unwrap_or_default()
    }
}
/// Returns the object key (name) of the S3 object.
pub trait HasObject: HasS3Fields {
    /// Returns the object key (name) of the S3 object.
    #[inline]
    fn object(&self) -> &str {
        //TODO consider using ObjectKey type
        self.request()
            .object
            .as_ref()
            .map(|o| o.as_str())
            .unwrap_or_default()
    }
}
/// Returns the region of the S3 bucket.
pub trait HasRegion: HasS3Fields {
    /// Returns the region of the S3 bucket.
    #[inline]
    fn region(&self) -> &str {
        // TODO consider using Region type
        self.request().inner_region.as_str()
    }
}

/// Returns the version ID of the object (`x-amz-version-id`), if versioning is enabled for the bucket.
pub trait HasVersion: HasS3Fields {
    /// Returns the version ID of the object (`x-amz-version-id`), if versioning is enabled for the bucket.
    #[inline]
    fn version_id(&self) -> Option<&str> {
        // TODO consider using VersionId type
        self.headers()
            .get(X_AMZ_VERSION_ID)
            .and_then(|v| v.to_str().ok())
    }
}

/// Returns the value of the `ETag` header from response headers (for operations that return ETag in headers).
/// The ETag is typically a hash of the object content, but it may vary based on the storage backend.
pub trait HasEtagFromHeaders: HasS3Fields {
    /// Returns the value of the `ETag` header from response headers (for operations that return ETag in headers).
    /// The ETag is typically a hash of the object content, but it may vary based on the storage backend.
    #[inline]
    fn etag(&self) -> Result<String, ValidationErr> {
        // Retrieve the ETag from the response headers.
        let etag: String = self // TODO create an etag struct
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"'))
            .unwrap_or_default()
            .to_string();
        Ok(etag)
    }
}

/// Returns the value of the `ETag` from the response body, which is a unique identifier for
/// the object version. The ETag is typically a hash of the object content, but it may vary
/// based on the storage backend.
pub trait HasEtagFromBody: HasS3Fields {
    /// Returns the value of the `ETag` from the response body, which is a unique identifier for
    /// the object version. The ETag is typically a hash of the object content, but it may vary
    /// based on the storage backend.
    fn etag(&self) -> Result<String, ValidationErr> {
        // Retrieve the ETag from the response body.
        let root = xmltree::Element::parse(self.body().clone().reader())?;
        let etag: String = get_text_result(&root, "ETag")?;
        Ok(trim_quotes(etag))
    }
}

/// Returns the size of the object in bytes, as specified by the `x-amz-object-size` header.
pub trait HasObjectSize: HasS3Fields {
    /// Returns the size of the object in bytes, as specified by the `x-amz-object-size` header.
    #[inline]
    fn object_size(&self) -> u64 {
        self.headers()
            .get(X_AMZ_OBJECT_SIZE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    }
}

/// Provides access to the `x-amz-delete-marker` header value.
///
/// Indicates whether the specified object version that was permanently deleted was (true) or
/// was not (false) a delete marker before deletion. In a simple DELETE, this header indicates
/// whether (true) or not (false) the current version of the object is a delete marker.
pub trait HasIsDeleteMarker: HasS3Fields {
    /// Returns `true` if the object is a delete marker, `false` otherwise.
    #[inline]
    fn is_delete_marker(&self) -> Result<bool, ValidationErr> {
        self.headers()
            .get(X_AMZ_DELETE_MARKER)
            .map_or(Ok(false), |v| parse_bool(v.to_str()?))
    }
}

pub trait HasTagging: HasS3Fields {
    /// Returns the tags associated with the bucket.
    ///
    /// If the bucket has no tags, this will return an empty `HashMap`.
    #[inline]
    fn tags(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let mut tags = HashMap::new();
        if self.body().is_empty() {
            // Note: body is empty when server responses with NoSuchTagSet
            return Ok(tags);
        }
        let mut root = Element::parse(self.body().clone().reader())?;
        let element = root
            .get_mut_child("TagSet")
            .ok_or(ValidationErr::xml_error("<TagSet> tag not found"))?;
        while let Some(v) = element.take_child("Tag") {
            tags.insert(get_text_result(&v, "Key")?, get_text_result(&v, "Value")?);
        }
        Ok(tags)
    }
}

/// Provides checksum-related methods for S3 responses with headers.
///
/// This trait provides default implementations for extracting and detecting checksums
/// from S3 response headers. Implement this trait for any response type that has
/// `HeaderMap` access via `HasS3Fields`.
pub trait HasChecksumHeaders: HasS3Fields {
    /// Extracts the checksum value from response headers for the specified algorithm.
    ///
    /// Retrieves the base64-encoded checksum value from the appropriate S3 response header
    /// (x-amz-checksum-crc32, x-amz-checksum-crc32c, x-amz-checksum-crc64nvme,
    /// x-amz-checksum-sha1, or x-amz-checksum-sha256).
    ///
    /// # Arguments
    ///
    /// * `algorithm` - The checksum algorithm to retrieve
    ///
    /// # Returns
    ///
    /// - `Some(checksum)` if the header is present, containing the base64-encoded checksum value
    /// - `None` if the header is not found
    ///
    /// # Use Cases
    ///
    /// - Compare with locally computed checksums for manual verification
    /// - Store checksum values for audit or compliance records
    /// - Verify integrity after downloading to disk
    #[inline]
    fn get_checksum(&self, algorithm: ChecksumAlgorithm) -> Option<String> {
        let header_name = match algorithm {
            ChecksumAlgorithm::CRC32 => X_AMZ_CHECKSUM_CRC32,
            ChecksumAlgorithm::CRC32C => X_AMZ_CHECKSUM_CRC32C,
            ChecksumAlgorithm::SHA1 => X_AMZ_CHECKSUM_SHA1,
            ChecksumAlgorithm::SHA256 => X_AMZ_CHECKSUM_SHA256,
            ChecksumAlgorithm::CRC64NVME => X_AMZ_CHECKSUM_CRC64NVME,
        };

        self.headers()
            .get(header_name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    /// Returns the checksum type from response headers.
    ///
    /// The checksum type indicates whether the checksum represents:
    /// - `FULL_OBJECT` - A checksum computed over the entire object
    /// - `COMPOSITE` - A checksum-of-checksums for multipart uploads
    ///
    /// # Returns
    ///
    /// - `Some(type_string)` if the `x-amz-checksum-type` header is present
    /// - `None` if the header is not found
    #[inline]
    fn checksum_type(&self) -> Option<String> {
        self.headers()
            .get(X_AMZ_CHECKSUM_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    /// Detects which checksum algorithm was used by the server (if any).
    ///
    /// Examines response headers to determine if the server computed a checksum
    /// for this operation.
    ///
    /// # Returns
    ///
    /// - `Some(algorithm)` if a checksum header is found (CRC32, CRC32C, CRC64NVME, SHA1, or SHA256)
    /// - `None` if no checksum headers are present
    #[inline]
    fn detect_checksum_algorithm(&self) -> Option<ChecksumAlgorithm> {
        if self.headers().contains_key(X_AMZ_CHECKSUM_CRC32) {
            Some(ChecksumAlgorithm::CRC32)
        } else if self.headers().contains_key(X_AMZ_CHECKSUM_CRC32C) {
            Some(ChecksumAlgorithm::CRC32C)
        } else if self.headers().contains_key(X_AMZ_CHECKSUM_CRC64NVME) {
            Some(ChecksumAlgorithm::CRC64NVME)
        } else if self.headers().contains_key(X_AMZ_CHECKSUM_SHA1) {
            Some(ChecksumAlgorithm::SHA1)
        } else if self.headers().contains_key(X_AMZ_CHECKSUM_SHA256) {
            Some(ChecksumAlgorithm::SHA256)
        } else {
            None
        }
    }
}
