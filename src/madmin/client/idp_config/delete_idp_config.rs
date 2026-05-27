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

use crate::madmin::builders::{DeleteIdpConfig, DeleteIdpConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Delete an IDP (Identity Provider) configuration.
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
    ///     .delete_idp_config()
    ///     .idp_type(IdpType::OpenId)
    ///     .name("myidp")
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// if response.restart_required() {
    ///     println!("Server restart required for deletion to take effect");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_idp_config(&self) -> DeleteIdpConfigBldr {
        DeleteIdpConfig::builder().client(self.clone())
    }
}
