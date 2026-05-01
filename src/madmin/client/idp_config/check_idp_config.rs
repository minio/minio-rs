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

use crate::madmin::builders::{CheckIdpConfig, CheckIdpConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Validate an IDP (Identity Provider) configuration.
    ///
    /// This operation checks if the IDP configuration is valid and can connect
    /// to the identity provider. This is primarily used for LDAP configurations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::idp_config::IdpType;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let response = madmin
    ///     .check_idp_config()
    ///     .idp_type(IdpType::Ldap)
    ///     .name("myldap")
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// if response.is_valid() {
    ///     println!("IDP configuration is valid");
    /// } else {
    ///     let result = response.result();
    ///     println!("Validation failed: {:?}", result.error_message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn check_idp_config(&self) -> CheckIdpConfigBldr {
        CheckIdpConfig::builder().client(self.clone())
    }
}
