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

use crate::madmin::builders::{ClusterAPIStats, ClusterAPIStatsBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get cluster-wide API statistics
    ///
    /// Returns general API metrics for the cluster including active/queued requests,
    /// error counts, request counts, and durations for the last minute and last day.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::types::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url: BaseUrl = "http://localhost:9000".parse()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let stats = client.cluster_api_stats().send().await?;
    /// println!("Active requests: {}", stats.stats.active_requests);
    /// println!("Queued requests: {}", stats.stats.queued_requests);
    /// println!("Nodes reporting: {}", stats.stats.nodes);
    /// # Ok(())
    /// # }
    /// ```
    pub fn cluster_api_stats(&self) -> ClusterAPIStatsBldr {
        ClusterAPIStats::builder().client(self.clone())
    }
}
