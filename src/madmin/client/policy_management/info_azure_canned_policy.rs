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

use crate::madmin::builders::{InfoAzureCannedPolicy, InfoAzureCannedPolicyBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a builder for getting Azure-specific canned policy information.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::types::policy::InfoAzureCannedPolicyReq;
    /// # async fn example(client: minio::madmin::madmin_client::MadminClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let req = InfoAzureCannedPolicyReq {
    ///     name: "my-azure-policy".to_string(),
    ///     config_name: "azure-config".to_string(),
    /// };
    ///
    /// let response = client
    ///     .info_azure_canned_policy()
    ///     .request(req)
    ///     .send()
    ///     .await?;
    ///
    /// println!("Group: {}", response.info.group_name);
    /// # Ok(())
    /// # }
    /// ```
    pub fn info_azure_canned_policy(&self) -> InfoAzureCannedPolicyBldr {
        InfoAzureCannedPolicy::builder().client(self.clone())
    }
}
