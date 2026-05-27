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

use crate::madmin::builders::{GetConfig, GetConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieves the current server configuration.
    ///
    /// This returns the complete MinIO server configuration in raw bytes format.
    /// The configuration is encrypted during transmission and automatically decrypted.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains the decrypted configuration bytes.
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
    /// let config_bytes = madmin.get_config().send().await?;
    /// let config_str = String::from_utf8_lossy(&config_bytes);
    /// println!("Server configuration:\n{}", config_str);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Configuration is returned in MinIO's internal format (typically key-value pairs)
    /// - Use with caution as configuration may contain sensitive information
    pub fn get_config(&self) -> GetConfigBldr {
        GetConfig::builder().client(self.clone())
    }
}
