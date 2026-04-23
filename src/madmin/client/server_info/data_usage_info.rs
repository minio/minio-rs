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

use crate::madmin::builders::{DataUsageInfo, DataUsageInfoBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get data usage information for the cluster
    ///
    /// Returns statistics about object counts, total size, per-bucket usage,
    /// tiering stats, replication metrics, and cluster capacity.
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
    /// let info = client.data_usage_info().send().await?;
    /// println!("Total objects: {}", info.info.objects_count);
    /// println!("Total size: {} bytes", info.info.objects_total_size);
    /// println!("Number of buckets: {}", info.info.buckets_count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn data_usage_info(&self) -> DataUsageInfoBldr {
        DataUsageInfo::builder().client(self.clone())
    }
}
