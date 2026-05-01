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

use crate::madmin::builders::{RemoveAzureCannedPolicy, RemoveAzureCannedPolicyBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a builder for removing an Azure-specific canned policy.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::types::policy::RemoveAzureCannedPolicyReq;
    /// # async fn example(client: minio::madmin::madmin_client::MadminClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let req = RemoveAzureCannedPolicyReq {
    ///     name: "my-azure-policy".to_string(),
    ///     config_name: "azure-config".to_string(),
    /// };
    ///
    /// let response = client
    ///     .remove_azure_canned_policy()
    ///     .request(req)
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_azure_canned_policy(&self) -> RemoveAzureCannedPolicyBldr {
        RemoveAzureCannedPolicy::builder().client(self.clone())
    }
}
