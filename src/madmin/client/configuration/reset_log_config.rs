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

use crate::madmin::builders::{ResetLogConfig, ResetLogConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Resets the log configuration to default values.
    ///
    /// Disables all log recorders and clears their configuration settings.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to send the request.
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
    /// madmin.reset_log_config().send().await?;
    /// println!("Log configuration reset to defaults");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Disables all log recording (API, Error, and Audit)
    /// - Clears drive limits and flush settings
    pub fn reset_log_config(&self) -> ResetLogConfigBldr {
        ResetLogConfig::builder().client(self.clone())
    }
}
