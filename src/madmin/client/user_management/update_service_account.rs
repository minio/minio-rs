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

use crate::madmin::builders::{UpdateServiceAccount, UpdateServiceAccountBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Updates an existing service account's properties.
    ///
    /// This allows modifying the policy, secret key, status, name, description,
    /// or expiration of an existing service account.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::service_account::UpdateServiceAccountReq;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let request = UpdateServiceAccountReq {
    ///     new_policy: None,
    ///     new_secret_key: Some("newsecretkey".to_string()),
    ///     new_status: Some("disabled".to_string()),
    ///     new_name: Some("updated-name".to_string()),
    ///     new_description: Some("Updated description".to_string()),
    ///     new_expiration: None,
    /// };
    ///
    /// let response = madmin
    ///     .update_service_account()
    ///     .access_key("service-account-key")
    ///     .request(request)
    ///     .send()
    ///     .await?;
    ///
    /// if response.success {
    ///     println!("Service account updated successfully");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Only the fields set in the request will be updated
    /// - The status field must be either "enabled" or "disabled"
    /// - Name must be <= 32 characters and start with a letter
    /// - Description must be <= 256 bytes
    /// - Expiration must be in the future
    pub fn update_service_account(&self) -> UpdateServiceAccountBldr {
        UpdateServiceAccount::builder().client(self.clone())
    }
}
