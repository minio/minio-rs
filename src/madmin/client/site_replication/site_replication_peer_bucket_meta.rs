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

use crate::madmin::builders::{SiteReplicationPeerBucketMeta, SiteReplicationPeerBucketMetaBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get or manage bucket metadata for peer sites in replication
    ///
    /// Retrieves or modifies bucket metadata on peer sites in the replication configuration.
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
    ///     .site_replication_peer_bucket_meta()
    ///     .send()
    ///     .await?;
    ///
    /// if response.success {
    ///     println!("Bucket metadata retrieved from peer");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn site_replication_peer_bucket_meta(&self) -> SiteReplicationPeerBucketMetaBldr {
        SiteReplicationPeerBucketMeta::builder().client(self.clone())
    }
}
