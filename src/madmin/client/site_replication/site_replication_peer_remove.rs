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

use crate::madmin::builders::{SiteReplicationPeerRemove, SiteReplicationPeerRemoveBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Remove a peer from site replication (server-to-server operation)
    ///
    /// This is an internal operation used by MinIO servers for peer-to-peer
    /// communication during site replication removal. This API is called by
    /// one MinIO server to notify another server to remove sites from its
    /// replication configuration.
    ///
    /// **Note:** This is a low-level internal API. Most users should use
    /// `site_replication_remove()` instead, which is the client-facing API
    /// for removing sites from replication.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::madmin::types::site_replication::SRRemoveReq;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "localhost:9000".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("admin", "password", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let remove_req = SRRemoveReq {
    ///     requesting_dep_id: "dep-id-123".to_string(),
    ///     site_names: vec!["site1".to_string()],
    ///     remove_all: false,
    /// };
    ///
    /// let response = client
    ///     .site_replication_peer_remove()
    ///     .req(remove_req)
    ///     .send()
    ///     .await?;
    ///
    /// println!("Peer removal: {}", response.status);
    /// # Ok(())
    /// # }
    /// ```
    pub fn site_replication_peer_remove(&self) -> SiteReplicationPeerRemoveBldr {
        SiteReplicationPeerRemove::builder().client(self.clone())
    }
}
