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

use crate::madmin::builders::{GetGroupDescription, GetGroupDescriptionBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieves detailed information about a group.
    ///
    /// This returns comprehensive information about a group including its members,
    /// status, attached policy, and last update timestamp.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains detailed group information.
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
    /// let group_info = madmin
    ///     .get_group_description()
    ///     .group("developers")
    ///     .send()
    ///     .await?;
    ///
    /// println!("Group: {}", group_info.name);
    /// println!("Status: {}", group_info.status);
    /// println!("Members: {:?}", group_info.members);
    /// println!("Policy: {}", group_info.policy);
    /// if let Some(updated) = group_info.updated_at {
    ///     println!("Last updated: {}", updated);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Returns error if group does not exist
    /// - Members list shows all users currently in the group
    /// - Policy field shows the policy attached to the group (if any)
    pub fn get_group_description(&self) -> GetGroupDescriptionBldr {
        GetGroupDescription::builder().client(self.clone())
    }
}
