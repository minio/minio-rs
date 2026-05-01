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

use crate::madmin::builders::{InfoServiceAccount, InfoServiceAccountBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::typed_parameters::AccessKey;
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Gets detailed information about a service account.
    ///
    /// Returns comprehensive information including parent user, status, policy,
    /// name, description, and expiration.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains detailed service account information.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let info = madmin
    ///     .info_service_account("service-account-key")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Parent User: {}", info.parent_user);
    /// println!("Status: {}", info.account_status);
    /// if let Some(policy) = info.policy {
    ///     println!("Policy: {}", policy);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn info_service_account<A>(
        &self,
        access_key: A,
    ) -> Result<InfoServiceAccountBldr, ValidationErr>
    where
        A: TryInto<AccessKey>,
        A::Error: Into<ValidationErr>,
    {
        Ok(InfoServiceAccount::builder()
            .client(self.clone())
            .access_key(access_key.try_into().map_err(Into::into)?))
    }
}
