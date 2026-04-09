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

use crate::madmin::builders::{AttachPolicyLDAP, AttachPolicyLDAPBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Attach policies to an LDAP user or group
    ///
    /// Associates one or more policies with an LDAP user or group. Either user or group must be specified, but not both.
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
    ///     // Attach policies to an LDAP user
    ///     let response = madmin_client
    ///         .attach_policy_ldap()
    ///         .policies(vec!["readwrite".to_string()])
    ///         .user("cn=testuser,ou=users,dc=example,dc=com".to_string())
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Attached policies: {:?}", response.as_ref().policies_attached);
    ///     Ok(())
    /// }
    /// ```
    pub fn attach_policy_ldap(&self) -> AttachPolicyLDAPBldr {
        AttachPolicyLDAP::builder().client(self.clone())
    }
}
