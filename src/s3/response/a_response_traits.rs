use crate::s3::error::{MinioError, Result};
use crate::s3::types::S3Request;
use crate::s3::utils::{get_text, trim_quotes};
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
            impl FromS3Response for $ty {
                async fn from_s3response(
                    request: S3Request,
                    response: Result<reqwest::Response>,
                ) -> Result<Self> {
                    let mut resp: reqwest::Response = response?;
                    Ok(Self {
                        request,
                        headers: mem::take(resp.headers_mut()),
                        body: resp.bytes().await?
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
            impl FromS3Response for $ty {
                async fn from_s3response(
                    request: S3Request,
                    response: Result<reqwest::Response>,
                ) -> Result<Self> {
                    let mut resp: reqwest::Response = response?;
                    Ok(Self {
                        request,
                        headers: mem::take(resp.headers_mut()),
                        body: resp.bytes().await?,
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
            impl HasS3Fields for $ty {
                /// The request that was sent to the S3 API.
                fn request(&self) -> &S3Request {
                    &self.request
                }

                /// The response of the S3 API.
                fn headers(&self) -> &HeaderMap {
                    &self.headers
                }

                /// The response of the S3 API.
                fn body(&self) -> &Bytes {
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
        self.request().bucket.as_deref().unwrap_or_default()
    }
}
/// Returns the object key (name) of the S3 object.
pub trait HasObject: HasS3Fields {
    /// Returns the object key (name) of the S3 object.
    #[inline]
    fn object(&self) -> &str {
        self.request().object.as_deref().unwrap_or_default()
    }
}
/// Returns the region of the S3 bucket.
pub trait HasRegion: HasS3Fields {
    /// Returns the region of the S3 bucket.
    #[inline]
    fn region(&self) -> &str {
        &self.request().inner_region
    }
}

/// Returns the version ID of the object (`x-amz-version-id`), if versioning is enabled for the bucket.
pub trait HasVersion: HasS3Fields {
    /// Returns the version ID of the object (`x-amz-version-id`), if versioning is enabled for the bucket.
    #[inline]
    fn version_id(&self) -> Option<&str> {
        self.headers()
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok())
    }
}

/// Returns the value of the `ETag` header from response headers (for operations that return ETag in headers).
/// The ETag is typically a hash of the object content, but it may vary based on the storage backend.
pub trait HasEtagFromHeaders: HasS3Fields {
    /// Returns the value of the `ETag` header from response headers (for operations that return ETag in headers).
    /// The ETag is typically a hash of the object content, but it may vary based on the storage backend.
    #[inline]
    fn etag(&self) -> Result<String> {
        // Retrieve the ETag from the response headers.
        let etag = self
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
    fn etag(&self) -> Result<String> {
        // Retrieve the ETag from the response body.
        let root = xmltree::Element::parse(self.body().clone().reader())?;
        let etag: String = get_text(&root, "ETag")?;
        Ok(trim_quotes(etag))
    }
}

/// Returns the size of the object in bytes, as specified by the `x-amz-object-size` header.
pub trait HasObjectSize: HasS3Fields {
    /// Returns the size of the object in bytes, as specified by the `x-amz-object-size` header.
    #[inline]
    fn object_size(&self) -> u64 {
        self.headers()
            .get("x-amz-object-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    }
}

/// Value of the `x-amz-delete-marker` header.
/// Indicates whether the specified object version that was permanently deleted was (true) or
/// was not (false) a delete marker before deletion. In a simple DELETE, this header indicates
/// whether (true) or not (false) the current version of the object is a delete marker.
pub trait HasIsDeleteMarker: HasS3Fields {
    /// Returns `true` if the object is a delete marker, `false` otherwise.
    ///
    /// Value of the `x-amz-delete-marker` header.
    /// Indicates whether the specified object version that was permanently deleted was (true) or
    /// was not (false) a delete marker before deletion. In a simple DELETE, this header indicates
    /// whether (true) or not (false) the current version of the object is a delete marker.
    #[inline]
    fn is_delete_marker(&self) -> Result<Option<bool>> {
        Ok(Some(
            self.headers()
                .get("x-amz-delete-marker")
                .map(|v| v == "true")
                .unwrap_or(false),
        ))

        //Ok(match self.headers().get("x-amz-delete-marker") {
        //    Some(v) => Some(v.to_str()?.parse::<bool>()?),
        //    None => None,
        //})
    }
}

pub trait HasTagging: HasS3Fields {
    /// Returns the tags associated with the bucket.
    ///
    /// If the bucket has no tags, this will return an empty `HashMap`.
    #[inline]
    fn tags(&self) -> Result<HashMap<String, String>> {
        let mut tags = HashMap::new();
        if self.body().is_empty() {
            // Note: body is empty when server responses with NoSuchTagSet
            return Ok(tags);
        }
        let mut root = Element::parse(self.body().clone().reader())?;
        let element = root
            .get_mut_child("TagSet")
            .ok_or(MinioError::XmlError("<TagSet> tag not found".to_string()))?;
        while let Some(v) = element.take_child("Tag") {
            tags.insert(get_text(&v, "Key")?, get_text(&v, "Value")?);
        }
        Ok(tags)
    }
}
