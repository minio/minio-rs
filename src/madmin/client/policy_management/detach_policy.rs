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

use crate::madmin::builders::{DetachPolicy, DetachPolicyBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Detaches one or more policies from a user or group.
    ///
    /// This removes the association between IAM policies and users or groups,
    /// revoking the permissions defined in those policies.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains the list of detached policies and update timestamp.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::policy::PolicyAssociationReq;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// let request = PolicyAssociationReq {
    ///     policies: vec!["readonly".to_string()],
    ///     user: Some("testuser".to_string()),
    ///     group: None,
    ///     config_name: None,
    /// };
    ///
    /// let response = madmin.detach_policy().request(request).send().await?;
    /// if let Some(detached) = response.policies_detached {
    ///     println!("Detached policies: {:?}", detached);
    /// }
    /// println!("Updated at: {}", response.updated_at);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Either `user` or `group` must be specified in the request, but not both
    /// - Multiple policies can be detached in a single request
    /// - Detaching a policy that isn't attached is not an error
    pub fn detach_policy(&self) -> DetachPolicyBldr {
        DetachPolicy::builder().client(self.clone())
    }
}
