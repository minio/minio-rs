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

use crate::madmin::builders::{InfoCannedPolicy, InfoCannedPolicyBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Gets detailed information about a specific IAM policy.
    ///
    /// Returns the policy document along with metadata such as creation
    /// and update timestamps.
    ///
    /// # Arguments
    ///
    /// * `policy_name` - The name of the policy to retrieve
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains the policy information including the JSON document.
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
    ///     .info_canned_policy()
    ///     .policy_name("readonly-policy")
    ///     .send()
    ///     .await?;
    ///
    /// println!("Policy: {}", info.policy_name);
    /// println!("Document: {}", info.policy);
    /// if let Some(created) = info.create_date {
    ///     println!("Created: {}", created);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn info_canned_policy(&self) -> InfoCannedPolicyBldr {
        InfoCannedPolicy::builder().client(self.clone())
    }
}
