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

use crate::madmin::builders::{AddServiceAccount, AddServiceAccountBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a new service account with the specified configuration.
    ///
    /// Service accounts are credentials that can be used to access MinIO with restricted
    /// permissions defined by a policy. They belong to the user making the request and
    /// can have an optional expiration time.
    ///
    /// # Arguments
    ///
    /// * `request` - The service account configuration including optional policy, target user,
    ///   credentials, name, description, and expiration
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains the credentials (access key and secret key) for the new service account.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::service_account::AddServiceAccountReq;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let request = AddServiceAccountReq {
    ///     policy: None,
    ///     target_user: None,
    ///     access_key: None,
    ///     secret_key: None,
    ///     name: Some("my-service-account".to_string()),
    ///     description: Some("Service account for application X".to_string()),
    ///     expiration: None,
    /// };
    ///
    /// let response = madmin.add_service_account()
    ///     .request(request)
    ///     .send().await?;
    /// println!("Access Key: {}", response.credentials.access_key);
    /// println!("Secret Key: {}", response.credentials.secret_key);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - The service account will belong to the authenticated user making the request
    /// - If `access_key` is not specified, MinIO will generate one automatically
    /// - If `secret_key` is not specified, MinIO will generate one automatically
    /// - The `policy` field can be used to restrict the service account's permissions
    /// - The `expiration` field defines when the service account credentials expire
    /// - Name must be <= 32 characters and start with a letter
    /// - Description must be <= 256 bytes
    pub fn add_service_account(&self) -> AddServiceAccountBldr {
        AddServiceAccount::builder().client(self.clone())
    }
}
