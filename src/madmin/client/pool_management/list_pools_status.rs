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

use crate::madmin::builders::pool_management::ListPoolsStatus;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// List all storage pools and their status.
    ///
    /// Returns a list of all pools currently configured and being used on the cluster,
    /// including their decommissioning status if applicable.
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
    ///     let pools = madmin.list_pools_status().send().await?;
    ///     for pool in pools {
    ///         println!("Pool {}: {} (decommissioning: {})",
    ///             pool.id, pool.cmdline, pool.is_decommissioning());
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn list_pools_status(&self) -> ListPoolsStatus {
        ListPoolsStatus::builder().client(self.clone()).build()
    }
}
