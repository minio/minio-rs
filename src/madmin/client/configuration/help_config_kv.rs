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

use crate::madmin::builders::{HelpConfigKV, HelpConfigKVBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieves help information for configuration subsystems.
    ///
    /// This returns documentation about available configuration keys and their options
    /// for a specific subsystem or configuration key.
    ///
    /// # Arguments
    ///
    /// * `sub_sys` - The subsystem name (e.g., "notify_webhook", "compression", "region")
    /// * `key` - Optional: Specific configuration key within the subsystem
    /// * `env_only` - Optional: Set to true to retrieve only environment variable help
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains Help information with descriptions, types, and requirements.
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
    /// // Get help for compression subsystem
    /// let help_response = madmin.help_config_kv()
    ///     .sub_sys("compression")
    ///     .send()
    ///     .await?;
    ///
    /// let help = help_response.help();
    /// println!("Subsystem: {}", help.sub_sys);
    /// println!("Description: {}", help.description);
    ///
    /// for key_help in &help.keys_help {
    ///     println!("  Key: {}", key_help.key);
    ///     println!("  Description: {}", key_help.description);
    ///     println!("  Type: {}", key_help.type_);
    ///     println!("  Optional: {}", key_help.optional);
    /// }
    ///
    /// // Get help for a specific key
    /// let key_help = madmin.help_config_kv()
    ///     .sub_sys("compression")
    ///     .key("enable")
    ///     .send()
    ///     .await?;
    ///
    /// // Get environment variable help only
    /// let env_help = madmin.help_config_kv()
    ///     .sub_sys("compression")
    ///     .env_only(true)
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Use this to discover available configuration options programmatically
    /// - Useful for building configuration UIs or validation tools
    /// - The help includes type information, descriptions, and whether fields are optional
    pub fn help_config_kv(&self) -> HelpConfigKVBldr {
        HelpConfigKV::builder().client(self.clone())
    }
}
