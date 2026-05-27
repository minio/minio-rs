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

use crate::madmin::builders::{DeleteServiceAccount, DeleteServiceAccountBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::typed_parameters::AccessKey;
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Deletes a service account by its access key.
    ///
    /// This permanently removes the service account and revokes its credentials.
    /// The operation cannot be undone.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
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
    /// let response = madmin
    ///     .delete_service_account("service-account-key")?
    ///     .send()
    ///     .await?;
    ///
    /// if response.success {
    ///     println!("Service account deleted successfully");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_service_account<A>(
        &self,
        access_key: A,
    ) -> Result<DeleteServiceAccountBldr, ValidationErr>
    where
        A: TryInto<AccessKey>,
        A::Error: Into<ValidationErr>,
    {
        Ok(DeleteServiceAccount::builder()
            .client(self.clone())
            .access_key(access_key.try_into().map_err(Into::into)?))
    }
}
