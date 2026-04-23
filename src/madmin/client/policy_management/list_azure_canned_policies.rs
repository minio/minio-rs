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

use crate::madmin::builders::{ListAzureCannedPolicies, ListAzureCannedPoliciesBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a builder for listing Azure-specific canned policies.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::types::policy::ListAzureCannedPoliciesReq;
    /// # async fn example(client: minio::madmin::madmin_client::MadminClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let req = ListAzureCannedPoliciesReq {
    ///     config_name: Some("azure-config".to_string()),
    ///     get_all_uuids: None,
    /// };
    ///
    /// let response = client
    ///     .list_azure_canned_policies()
    ///     .request(req)
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_azure_canned_policies(&self) -> ListAzureCannedPoliciesBldr {
        ListAzureCannedPolicies::builder().client(self.clone())
    }
}
