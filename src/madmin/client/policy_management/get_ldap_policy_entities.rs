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

use crate::madmin::builders::{GetLDAPPolicyEntities, GetLDAPPolicyEntitiesBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get LDAP policy entities showing relationships between LDAP users, groups, and policies
    ///
    /// Returns LDAP policy entity associations based on the provided query criteria.
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
    ///     // Query LDAP policy entities for a specific policy
    ///     let response = madmin_client
    ///         .get_ldap_policy_entities()
    ///         .policy(vec!["readwrite".to_string()])
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     println!("LDAP policy entities: {:?}", response.as_ref());
    ///     Ok(())
    /// }
    /// ```
    pub fn get_ldap_policy_entities(&self) -> GetLDAPPolicyEntitiesBldr {
        GetLDAPPolicyEntities::builder().client(self.clone())
    }
}
