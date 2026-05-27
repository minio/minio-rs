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

use crate::madmin::builders::{GetLogConfig, GetLogConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieves the current log configuration.
    ///
    /// Returns the status of API, Error, and Audit log recorders including
    /// their enabled state, drive limits, flush counts, and flush intervals.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to send the request.
    /// The response contains LogStatus with configuration for all log types.
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
    /// let config = madmin.get_log_config().send().await?;
    /// println!("API logs enabled: {}", config.status().api.enabled);
    /// println!("Error logs enabled: {}", config.status().error.enabled);
    /// println!("Audit logs enabled: {}", config.status().audit.enabled);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Response is encrypted during transmission
    /// - Returns default values if log recording is not configured
    pub fn get_log_config(&self) -> GetLogConfigBldr {
        GetLogConfig::builder().client(self.clone())
    }
}
