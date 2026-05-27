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
//!
//! The [`MinioAdminClient`] provides access to MinIO-specific administrative operations
//! that are not part of the standard AWS S3 API. It wraps a regular [`crate::s3::client::MinioClient`]
//! and provides specialized methods for admin tasks.
//!
//! # Construction
//!
//! Create an admin client from a regular MinIO client:
//!
//! ```no_run
//! use minio::s3::MinioClient;
//! use minio::s3::creds::StaticProvider;
//! use minio::s3::http::BaseUrl;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
//! let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
//! let client = MinioClient::new(base_url, Some(static_provider), None, None)?;
//!
//! // Get the admin client
//! let admin = client.admin();
//! # Ok(())
//! # }
//! ```
//!
//! # Available Operations
//!
//! Currently supported operations:
//! - Inventory job control: suspend, resume, cancel
//!
//! # Architecture
//!
//! The admin client serves as a thin wrapper that provides convenient methods
//! for building admin API requests. Each method returns a builder type that
//! implements the [`crate::madmin::types::MadminApi`] trait.

use crate::madmin::madmin_client::MadminClient;
use crate::s3::client::MinioClient;

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

    /// Returns the underlying MadminClient for advanced operations.
    pub fn madmin_client(&self) -> MadminClient {
        let s3_client_shared = &self.base_client.shared;
        MadminClient::from_shared(
            s3_client_shared.base_url.clone(),
            s3_client_shared.provider.clone(),
        )
    }
}
