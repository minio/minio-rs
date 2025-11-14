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

//! Core types and traits for admin API operations.

use crate::s3::error::{Error, ValidationErr};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;

/// Trait for types that can be converted into admin API requests.
pub trait ToAdminRequest {
    /// Converts this type into an S3Request configured for admin API.
    fn to_admin_request(self) -> Result<S3Request, ValidationErr>;
}

/// Trait for admin API operations.
#[async_trait]
pub trait AdminApi: ToAdminRequest + Sized {
    /// The response type for this admin API operation.
    type Response: FromS3Response;

    /// Executes the admin API request and returns the response.
    async fn send(self) -> Result<Self::Response, Error> {
        let mut request = self.to_admin_request()?;
        let response = request.execute().await;
        Self::Response::from_s3response(request, Ok(response?)).await
    }
}
