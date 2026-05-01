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

use crate::madmin::builders::{KmsStatus, KmsStatusBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieve KMS (Key Management Service) status from MinIO
    ///
    /// Returns information about the configured KMS, including
    /// endpoints, default key, and other KMS-related details.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MadminClient::new("http://localhost:9000".parse().unwrap(), Some(provider));
    ///
    ///     let status = client.kms_status()
    ///         .send()
    ///         .await
    ///         .expect("Failed to get KMS status");
    ///
    ///     println!("KMS: {}", status.status.name);
    /// }
    /// ```
    pub fn kms_status(&self) -> KmsStatusBldr {
        KmsStatus::builder().client(self.clone())
    }
}
