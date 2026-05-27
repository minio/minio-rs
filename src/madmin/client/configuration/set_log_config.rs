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

use crate::madmin::builders::{SetLogConfig, SetLogConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Sets the log configuration for the server.
    ///
    /// Configures API, Error, and/or Audit log recorders with specified
    /// drive limits, flush counts, and flush intervals.
    ///
    /// # Arguments
    ///
    /// * `config` - LogConfig containing settings for log recorders
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to send the request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::log_config::{LogConfig, LogRecorderConfig};
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let config = LogConfig {
    ///     api: Some(LogRecorderConfig {
    ///         enable: true,
    ///         drive_limit: Some("1Gi".to_string()),
    ///         flush_count: Some(100),
    ///         flush_interval: Some("5s".to_string()),
    ///     }),
    ///     error: None,
    ///     audit: None,
    /// };
    ///
    /// madmin.set_log_config().config(config).send().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Request is encrypted during transmission
    /// - Only specified log types will be configured
    /// - Drive limits use human-readable format (e.g., "1Gi", "500Mi")
    pub fn set_log_config(&self) -> SetLogConfigBldr {
        SetLogConfig::builder().client(self.clone())
    }
}
