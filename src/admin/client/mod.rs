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

//! Admin API client for MinIO administrative operations.

use crate::s3::client::MinioClient;

mod cancel_inventory_job;
mod resume_inventory_job;
mod suspend_inventory_job;

/// MinIO Admin API client for administrative operations.
///
/// This client provides access to MinIO-specific admin operations
/// that are not part of the standard S3 API.
#[derive(Clone, Debug)]
pub struct MinioAdminClient {
    base_client: MinioClient,
}

impl MinioAdminClient {
    /// Creates a new admin client from a MinioClient.
    ///
    /// # Arguments
    ///
    /// * `client` - The base MinioClient to use for admin operations
    pub fn new(client: MinioClient) -> Self {
        Self {
            base_client: client,
        }
    }

    /// Returns a reference to the underlying MinioClient.
    pub fn base_client(&self) -> &MinioClient {
        &self.base_client
    }
}
