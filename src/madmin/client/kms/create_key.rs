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

use crate::madmin::builders::{KmsCreateKey, KmsCreateKeyBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Create a new cryptographic key in the KMS
    ///
    /// Creates a new key with the specified ID in the configured KMS server.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "localhost:9000".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("admin", "password", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let response = client
    ///     .create_key()
    ///     .key_id("my-encryption-key")
    ///     .send()
    ///     .await?;
    ///
    /// println!("Key created successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_key(&self) -> KmsCreateKeyBldr {
        KmsCreateKey::builder().client(self.clone())
    }
}
