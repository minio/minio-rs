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

use crate::madmin::builders::{BackgroundHealStatus, BackgroundHealStatusBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get background healing status for the cluster
    ///
    /// Returns information about ongoing background healing operations,
    /// including scanned items, healed disks, per-set healing status,
    /// and MRF (Most Recent Failures) metrics.
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
    /// let status = client.background_heal_status().send().await?;
    /// println!("Scanned items: {}", status.status.scanned_items_count);
    ///
    /// if let Some(sets) = &status.status.sets {
    ///     for set_status in sets {
    ///         println!("Pool {}, Set {}: {} objects healed",
    ///             set_status.pool, set_status.set, set_status.objects_healed);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn background_heal_status(&self) -> BackgroundHealStatusBldr {
        BackgroundHealStatus::builder().client(self.clone())
    }
}
