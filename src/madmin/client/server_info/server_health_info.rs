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

use crate::madmin::builders::{ServerHealthInfo, ServerHealthInfoBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get comprehensive health information for the cluster
    ///
    /// Returns detailed diagnostics including system information (CPU, memory, disks,
    /// network, processes), MinIO configuration, replication status, and more.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::types::BaseUrl;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url: BaseUrl = "http://localhost:9000".parse()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let health = client
    ///     .server_health_info()
    ///     .minio_info(true)
    ///     .sys_cpu(true)
    ///     .deadline(Duration::from_secs(30))
    ///     .send()
    ///     .await?;
    ///
    /// println!("Health check version: {}", health.health.version);
    /// println!("Timestamp: {}", health.health.timestamp);
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_health_info(&self) -> ServerHealthInfoBldr {
        ServerHealthInfo::builder().client(self.clone())
    }
}
