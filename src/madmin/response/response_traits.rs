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

//! Response traits for MinIO Admin API responses.
//!
//! This module provides common traits for accessing fields from admin API responses,
//! similar to the S3 response traits but adapted for the admin API context.

use crate::madmin::types::MadminRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Macro to implement `FromMadminResponse` for response types with standard fields.
///
/// This provides a default implementation that stores the request, headers, and body.
/// Use this for responses that don't need custom response processing logic.
///
/// # Example
///
/// ```rust,ignore
/// use crate::impl_from_madmin_response;
///
/// #[derive(Debug, Clone)]
/// pub struct MyResponse {
///     request: MadminRequest,
///     headers: HeaderMap,
///     body: Bytes,
/// }
///
/// impl_from_madmin_response!(MyResponse);
/// impl_has_madmin_fields!(MyResponse);
/// ```
#[macro_export]
macro_rules! impl_from_madmin_response {
    ($($ty:ty),* $(,)?) => {
        $(
            #[async_trait::async_trait]
            impl $crate::madmin::types::FromMadminResponse for $ty {
                async fn from_madmin_response(
                    request: $crate::madmin::types::MadminRequest,
                    response: Result<reqwest::Response, $crate::s3::error::Error>,
                ) -> Result<Self, $crate::s3::error::Error> {
                    let mut resp: reqwest::Response = response?;
                    Ok(Self {
                        request,
                        headers: std::mem::take(resp.headers_mut()),
                        body: resp.bytes().await.map_err($crate::s3::error::ValidationErr::HttpError)?,
                    })
                }
            }
        )*
    };
}

/// Macro to implement `HasMadminFields` for response types that have
/// `request`, `headers`, and `body` fields.
///
/// # Example
///
/// ```rust,ignore
/// use crate::impl_has_madmin_fields;
///
/// #[derive(Debug, Clone)]
/// pub struct MyResponse {
///     request: MadminRequest,
///     headers: HeaderMap,
///     body: Bytes,
///     // ... other fields
/// }
///
/// impl_has_madmin_fields!(MyResponse);
/// ```
#[macro_export]
macro_rules! impl_has_madmin_fields {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::madmin::response::response_traits::HasMadminFields for $ty {
                /// The request that was sent to the Admin API.
                fn request(&self) -> &$crate::madmin::types::MadminRequest {
                    &self.request
                }

                /// HTTP headers returned by the server.
                fn headers(&self) -> &::http::HeaderMap {
                    &self.headers
                }

                /// The response body returned by the server.
                fn body(&self) -> &::bytes::Bytes {
                    &self.body
                }
            }
        )*
    };
}

/// Base trait for all Admin API responses that store request/response metadata.
///
/// This trait provides access to the original request, response headers, and body.
/// It forms the foundation for other convenience traits like `HasBucket`.
pub trait HasMadminFields {
    /// The request that was sent to the Admin API.
    fn request(&self) -> &MadminRequest;

    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, etc.
    fn headers(&self) -> &HeaderMap;

    /// The response body returned by the server.
    fn body(&self) -> &Bytes;
}

/// Trait for responses that operate on a specific bucket.
///
/// Provides convenient access to the bucket name from the request.
/// This is useful for admin operations that are bucket-specific, such as:
/// - Export/Import bucket metadata
/// - Bucket replication configuration
/// - Bucket quota management
pub trait HasBucket: HasMadminFields {
    /// Returns the name of the bucket this response relates to.
    ///
    /// Returns `None` if the operation was not bucket-specific.
    #[inline]
    fn bucket(&self) -> Option<&str> {
        self.request().get_bucket()
    }
}
