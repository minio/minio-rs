// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Core traits for S3 request and response handling.

use super::s3_request::S3Request;
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use futures_util::Stream;

/// Trait for converting a request builder into a concrete S3 HTTP request.
///
/// This trait is implemented by all S3 request builders and serves as an
/// intermediate step in the request execution pipeline. It enables the
/// conversion from a strongly typed request builder into a generic
/// [`S3Request`] that can be executed over HTTP.
///
/// The [`S3Api::send`] method uses this trait to convert request builders
/// into executable HTTP requests before sending them to the S3-compatible
/// service.
///
/// # See Also
///
/// * [`S3Api`] - The trait that uses `ToS3Request` as part of its request execution pipeline
/// * [`FromS3Response`] - The counterpart trait for converting HTTP responses into typed responses
///
pub trait ToS3Request: Sized {
    /// Consumes this request builder and returns a [`S3Request`].
    ///
    /// This method transforms the request builder into a concrete HTTP request
    /// that can be executed against an S3-compatible service. The transformation
    /// includes:
    ///
    /// * Setting the appropriate HTTP method (GET, PUT, POST, etc.)
    /// * Building the request URL with path and query parameters
    /// * Adding required headers (authentication, content-type, etc.)
    /// * Attaching the request body, if applicable
    ///
    /// # Returns
    ///
    /// * `Result<S3Request, ValidationErr>` - The executable S3 request on success,
    ///   or an error if the request cannot be built correctly.
    ///
    fn to_s3request(self) -> Result<S3Request, ValidationErr>;
}

/// Trait for converting HTTP responses into strongly typed S3 response objects.
///
/// This trait is implemented by all S3 response types in the SDK and provides
/// a way to parse and validate raw HTTP responses from S3-compatible services.
/// It works as the final step in the request execution pipeline, transforming
/// the HTTP layer response into a domain-specific response object with proper
/// typing and field validation.
///
/// # See Also
///
/// * [`S3Api`] - The trait that uses `FromS3Response` as part of its request execution pipeline
/// * [`ToS3Request`] - The counterpart trait for converting request builders into HTTP requests
#[async_trait]
pub trait FromS3Response: Sized {
    /// Asynchronously converts an HTTP response into a strongly typed S3 response.
    ///
    /// This method takes both the original S3 request and the HTTP response (or error)
    /// that resulted from executing that request. It then parses the response data
    /// and constructs a typed response object that provides convenient access to
    /// the response fields.
    ///
    /// The method handles both successful responses and error responses from the
    /// S3 service, transforming S3-specific errors into appropriate error types.
    ///
    /// # Parameters
    ///
    /// * `s3req` - The original S3 request that was executed
    /// * `resp` - The result of the HTTP request execution, which can be either a
    ///   successful response or an error
    ///
    /// # Returns
    ///
    /// * `Result<Self, Error>` - The typed response object on success, or an error
    ///   if the response cannot be parsed or represents an S3 service error
    ///
    async fn from_s3response(
        s3req: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error>;
}

/// Trait that defines a common interface for all S3 API request builders.
///
/// This trait is implemented by all request builders in the SDK and provides
/// a consistent way to send requests and get typed responses. It works in
/// conjunction with [`ToS3Request`] to convert the builder into a concrete
/// HTTP request and with [`FromS3Response`] to convert the HTTP response back
/// into a strongly typed S3 response object.
///
/// # Type Parameters
///
/// * `S3Response` - The specific response type associated with this request builder.
///   Must implement the [`FromS3Response`] trait.
///
#[async_trait]
pub trait S3Api: ToS3Request {
    /// The response type associated with this request builder.
    ///
    /// Each implementation of `S3Api` defines its own response type that will be
    /// returned by the `send()` method. This type must implement the [`FromS3Response`]
    /// trait to enable conversion from the raw HTTP response.
    type S3Response: FromS3Response;
    /// Sends the S3 API request and returns the corresponding typed response.
    ///
    /// This method consumes the request builder, converts it into a concrete HTTP
    /// request using [`ToS3Request::to_s3request`], executes the request, and then
    /// converts the HTTP response into the appropriate typed response using
    /// [`FromS3Response::from_s3response`].
    ///
    /// # Returns
    ///
    /// * `Result<Self::S3Response, Error>` - The typed S3 response on success,
    ///   or an error if the request failed at any stage.
    ///
    async fn send(self) -> Result<Self::S3Response, Error> {
        let mut req: S3Request = self.to_s3request()?;
        let resp: Result<reqwest::Response, Error> = req.execute().await;
        Self::S3Response::from_s3response(req, resp).await
    }
}

#[async_trait]
/// Trait for types that can be converted to a stream of items.
pub trait ToStream: Sized {
    type Item;
    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send>;
}
