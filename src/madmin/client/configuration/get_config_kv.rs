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

use crate::madmin::builders::{GetConfigKV, GetConfigKVBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieves a specific configuration key-value pair.
    ///
    /// This returns the value for a specific configuration key from the MinIO server.
    /// The response is encrypted during transmission and automatically decrypted.
    ///
    /// # Arguments
    ///
    /// * `key` - The configuration key to retrieve (e.g., "notify_webhook:1", "region")
    /// * `env` - Optional: Set to true to retrieve environment variable configuration
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains the decrypted configuration value as bytes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Get regular config
    /// let value_bytes = madmin.get_config_kv().key("region").send().await?;
    /// let value_str = String::from_utf8_lossy(&value_bytes);
    /// println!("Region configuration: {}", value_str);
    ///
    /// // Get environment variable config
    /// let env_config = madmin.get_config_kv().key("region").env(true).send().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Key format typically follows "subsystem:id" pattern (e.g., "notify_webhook:1")
    /// - Returns empty bytes if key does not exist
    /// - Use `.env(true)` to retrieve environment variable configuration instead of server config
    pub fn get_config_kv(&self) -> GetConfigKVBldr {
        GetConfigKV::builder().client(self.clone())
    }
}
