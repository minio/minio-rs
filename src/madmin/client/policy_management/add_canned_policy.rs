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

use crate::madmin::builders::{AddCannedPolicy, AddCannedPolicyBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Adds a new IAM policy to the MinIO server.
    ///
    /// Canned policies define permissions that can be attached to users and groups.
    /// The policy must be valid JSON following AWS IAM policy syntax.
    ///
    /// # Arguments
    ///
    /// * `policy_name` - The name for the new policy
    /// * `policy` - The policy document as JSON bytes
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
    /// let policy_json = r#"{
    ///     "Version": "2012-10-17",
    ///     "Statement": [
    ///         {
    ///             "Effect": "Allow",
    ///             "Action": ["s3:GetObject"],
    ///             "Resource": ["arn:aws:s3:::mybucket/*"]
    ///         }
    ///     ]
    /// }"#;
    ///
    /// let response = madmin
    ///     .add_canned_policy()
    ///     .policy_name("readonly-policy")
    ///     .policy(policy_json.as_bytes().to_vec())
    ///     .send()
    ///     .await?;
    ///
    /// if response.success {
    ///     println!("Policy added successfully");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Policy Format
    ///
    /// The policy must be a valid JSON document following AWS IAM policy syntax with:
    /// - Version: "2012-10-17"
    /// - Statement: Array of policy statements with Effect, Action, and Resource
    pub fn add_canned_policy(&self) -> AddCannedPolicyBldr {
        AddCannedPolicy::builder().client(self.clone())
    }
}
