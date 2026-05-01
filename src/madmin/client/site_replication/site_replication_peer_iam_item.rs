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

use crate::madmin::builders::{SiteReplicationPeerIAMItem, SiteReplicationPeerIAMItemBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get or manage IAM items for peer sites in replication
    ///
    /// Retrieves or modifies IAM-related items (users, groups, policies) on peer sites.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "localhost:9000".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("admin", "password", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let response = client
    ///     .site_replication_peer_iam_item()
    ///     .send()
    ///     .await?;
    ///
    /// if response.success {
    ///     println!("IAM items retrieved from peer");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn site_replication_peer_iam_item(&self) -> SiteReplicationPeerIAMItemBldr {
        SiteReplicationPeerIAMItem::builder().client(self.clone())
    }
}
