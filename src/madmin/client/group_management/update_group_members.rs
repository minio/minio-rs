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

use crate::madmin::builders::{UpdateGroupMembers, UpdateGroupMembersBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Adds or removes members from a group.
    ///
    /// This modifies group membership by adding or removing users.
    /// The operation is determined by the `is_remove` flag in the request.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::group::GroupAddRemove;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Add members to a group
    /// let add_request = GroupAddRemove::add_members(
    ///     "developers".to_string(),
    ///     vec!["alice".to_string(), "bob".to_string()],
    /// );
    /// madmin.update_group_members().request(add_request).send().await?;
    /// println!("Members added to group");
    ///
    /// // Remove a member from a group
    /// let remove_request = GroupAddRemove::remove_members(
    ///     "developers".to_string(),
    ///     vec!["charlie".to_string()],
    /// );
    /// madmin.update_group_members().request(remove_request).send().await?;
    /// println!("Member removed from group");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Adding a user already in the group is not an error
    /// - Removing a user not in the group is not an error
    /// - Group name and members list cannot be empty (validated)
    /// - Users must exist before being added to a group
    pub fn update_group_members(&self) -> UpdateGroupMembersBldr {
        UpdateGroupMembers::builder().client(self.clone())
    }
}
