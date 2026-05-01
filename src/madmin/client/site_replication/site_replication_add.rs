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

use crate::madmin::builders::{SiteReplicationAdd, SiteReplicationAddBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Add peer sites for site replication
    ///
    /// Creates or extends a site replication configuration across multiple MinIO deployments.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::madmin::types::site_replication::{PeerSite, SRAddOptions};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "localhost:9000".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("admin", "password", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let sites = vec![
    ///     PeerSite {
    ///         name: "site1".to_string(),
    ///         endpoint: vec!["http://minio1:9000".to_string()],
    ///         access_key: "admin".to_string(),
    ///         secret_key: "password".to_string(),
    ///     },
    /// ];
    ///
    /// let opts = SRAddOptions::new().with_disable_ilm_expiry(false);
    ///
    /// let response = client
    ///     .site_replication_add()
    ///     .sites(sites)
    ///     .options(opts)
    ///     .send()
    ///     .await?;
    ///
    /// if response.success {
    ///     println!("Site replication configured: {}", response.status);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn site_replication_add(&self) -> SiteReplicationAddBldr {
        SiteReplicationAdd::builder().client(self.clone())
    }
}
