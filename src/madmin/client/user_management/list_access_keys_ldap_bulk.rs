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

use crate::madmin::builders::{ListAccessKeysLDAPBulk, ListAccessKeysLDAPBulkBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// List access keys for LDAP users in bulk
    ///
    /// Retrieves access keys for specified LDAP users or all users. Cannot specify both userDNs and all=true simultaneously.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url: BaseUrl = "http://localhost:9000".parse()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let madmin_client = MadminClient::new(base_url, Some(provider));
    ///
    ///     // List access keys for specific LDAP users
    ///     let response = madmin_client
    ///         .list_access_keys_ldap_bulk()
    ///         .user_dns(vec!["cn=testuser,ou=users,dc=example,dc=com".to_string()])
    ///         .list_type("all".to_string())
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     for (user, keys) in &response.users_keys {
    ///         println!("User {}: {} access keys", user, keys.access_keys.len());
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_access_keys_ldap_bulk(&self) -> ListAccessKeysLDAPBulkBldr {
        ListAccessKeysLDAPBulk::builder().client(self.clone())
    }
}
