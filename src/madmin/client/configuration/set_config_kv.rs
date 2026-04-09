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

use crate::madmin::builders::{SetConfigKV, SetConfigKVBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Sets a configuration key-value pair.
    ///
    /// This updates a specific configuration key with a new value on the MinIO server.
    /// The request is encrypted during transmission.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response indicates whether a server restart is required for the changes to take effect.
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
    /// let response = madmin
    ///     .set_config_kv()
    ///     .kv_string("region name=us-west-2")
    ///     .send()
    ///     .await?;
    ///
    /// if response.restart_required {
    ///     println!("Configuration set. Server restart required for changes to take effect.");
    /// } else {
    ///     println!("Configuration set and applied immediately.");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Key-value format follows "subsystem key=value" pattern
    /// - Some configuration changes require a server restart (indicated in the response)
    /// - Invalid configuration will be rejected by the server
    pub fn set_config_kv(&self) -> SetConfigKVBldr {
        SetConfigKV::builder().client(self.clone())
    }
}
