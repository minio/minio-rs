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

use crate::madmin::builders::{GetIdpConfig, GetIdpConfigBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieve an IDP (Identity Provider) configuration.
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
    ///     .get_idp_config()
    ///     .idp_type(IdpType::OpenId)
    ///     .name("myidp")
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// let config = response.config();
    /// println!("IDP Type: {}", config.idp_type);
    /// for info in &config.info {
    ///     println!("{} = {}", info.key, info.value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_idp_config(&self) -> GetIdpConfigBldr {
        GetIdpConfig::builder().client(self.clone())
    }
}
