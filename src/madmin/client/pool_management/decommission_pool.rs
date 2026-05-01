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

use crate::madmin::builders::pool_management::{DecommissionPool, DecommissionPoolBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::PoolName;
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Start decommissioning a storage pool.
    ///
    /// Initiates the process of moving data from the specified pool to all other
    /// existing pools. This is a long-running operation that can be monitored using
    /// `status_pool()` and canceled using `cancel_decommission_pool()`.
    ///
    /// # Arguments
    ///
    /// * `pool` - Pool definition to decommission (e.g., "http://server{1...4}/disk{1...4}")
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::client::Client;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = Client::new(base_url, Some(Box::new(provider)), None, None)?;
    ///     let madmin = client.madmin();
    ///
    ///     let pool = "http://server{1...4}/disk{1...4}";
    ///
    ///     // Start decommissioning
    ///     madmin.decommission_pool(pool)?.send().await?;
    ///     println!("Decommissioning started");
    ///
    ///     // Monitor progress
    ///     loop {
    ///         let status = madmin.status_pool(pool)?.send().await?;
    ///         if let Some(decom) = status.decommission {
    ///             println!("Progress: {:.2}%", decom.percent_complete());
    ///             if decom.complete {
    ///                 break;
    ///             }
    ///         }
    ///         tokio::time::sleep(Duration::from_secs(5)).await;
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn decommission_pool<P>(&self, pool: P) -> Result<DecommissionPoolBldr, ValidationErr>
    where
        P: TryInto<PoolName>,
        P::Error: Into<ValidationErr>,
    {
        Ok(DecommissionPool::builder()
            .client(self.clone())
            .pool(pool.try_into().map_err(Into::into)?))
    }
}
