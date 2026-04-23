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

use crate::madmin::builders::{RebalanceStatus, RebalanceStatusBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get status of cluster rebalance operation.
    ///
    /// Returns detailed status including progress for each pool.
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
    /// let status = madmin.rebalance_status().send().await?;
    /// println!("Rebalance ID: {}", status.id);
    /// for pool in &status.pools {
    ///     println!("Pool {}: {} ({}% used)", pool.id, pool.status, pool.used);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn rebalance_status(&self) -> RebalanceStatusBldr {
        RebalanceStatus::builder().client(self.clone())
    }
}
