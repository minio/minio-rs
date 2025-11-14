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

use crate::madmin::builders::pool_management::{
    CancelDecommissionPool, CancelDecommissionPoolBldr,
};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::PoolName;
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Cancel an ongoing pool decommissioning process.
    ///
    /// Stops the decommissioning process for the specified pool and automatically
    /// makes the pool available for writing once canceled.
    ///
    /// # Arguments
    ///
    /// * `pool` - Pool definition to cancel decommissioning (e.g., "http://server{1...4}/disk{1...4}")
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::client::Client;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
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
    ///     // Check if pool is being decommissioned
    ///     let status = madmin.status_pool(pool)?.send().await?;
    ///     if status.is_decommissioning() {
    ///         // Cancel the decommissioning
    ///         madmin.cancel_decommission_pool(pool)?.send().await?;
    ///         println!("Decommissioning canceled - pool is now available for writes");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn cancel_decommission_pool<P>(
        &self,
        pool: P,
    ) -> Result<CancelDecommissionPoolBldr, ValidationErr>
    where
        P: TryInto<PoolName>,
        P::Error: Into<ValidationErr>,
    {
        Ok(CancelDecommissionPool::builder()
            .client(self.clone())
            .pool(pool.try_into().map_err(Into::into)?))
    }
}
