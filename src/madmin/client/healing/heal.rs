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

use crate::madmin::builders::{Heal, HealBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Initiate or monitor healing operations on buckets/objects
    ///
    /// Healing is used to repair data consistency issues in the cluster.
    /// You can start a new heal, check status, or stop an ongoing operation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::madmin::types::heal::{HealOpts, HealScanMode};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::types::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url: BaseUrl = "http://localhost:9000".parse()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// // Start healing a bucket
    /// let opts = HealOpts {
    ///     recursive: Some(true),
    ///     scan_mode: Some(HealScanMode::Normal),
    ///     ..Default::default()
    /// };
    ///
    /// let result = client
    ///     .heal()
    ///     .bucket("my-bucket")
    ///     .opts(opts)
    ///     .force_start(true)
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn heal(&self) -> HealBldr {
        Heal::builder().client(self.clone())
    }
}
