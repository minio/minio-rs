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

use crate::madmin::builders::{ListServiceAccounts, ListServiceAccountsBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Lists all service accounts for a user.
    ///
    /// Returns a list of service accounts with their metadata including status,
    /// parent user, and expiration information.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains a list of service accounts.
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
    /// let response = madmin.list_service_accounts()
    ///     .user("someuser")
    ///     .send()
    ///     .await?;
    /// for account in response.accounts {
    ///     println!("Access Key: {}, Status: {}", account.access_key, account.account_status);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_service_accounts(&self) -> ListServiceAccountsBldr {
        ListServiceAccounts::builder().client(self.clone())
    }
}
