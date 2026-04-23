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

use crate::madmin::builders::{SetConfig, SetConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Updates the server configuration with new settings.
    ///
    /// This replaces the MinIO server configuration with the provided configuration bytes.
    /// The configuration is encrypted during transmission.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
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
    /// // First, get the current config
    /// let current_config = madmin.get_config().send().await?;
    ///
    /// // Modify the config (in this example, we'll just use the same config)
    /// let new_config = current_config;
    ///
    /// // Set the updated config
    /// madmin.set_config().config_bytes(new_config).send().await?;
    /// println!("Configuration updated successfully");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Maximum configuration size is 256 KiB
    /// - Invalid configuration will be rejected by the server
    /// - Some configuration changes may require a server restart to take effect
    /// - Use with caution as incorrect configuration can affect server operation
    pub fn set_config(&self) -> SetConfigBldr {
        SetConfig::builder().client(self.clone())
    }
}
