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

use crate::madmin::builders::{SetGroupStatus, SetGroupStatusBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Enables or disables a group.
    ///
    /// This changes the status of a group to enabled or disabled.
    /// Disabled groups remain configured but their members cannot use group permissions.
    ///
    /// # Arguments
    ///
    /// * `group` - The name of the group to modify
    /// * `status` - The new status (enabled or disabled)
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::group::GroupStatus;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Disable a group
    /// madmin
    ///     .set_group_status()
    ///     .group("developers")
    ///     .status(GroupStatus::Disabled)
    ///     .send()
    ///     .await?;
    /// println!("Group disabled");
    ///
    /// // Re-enable a group
    /// madmin
    ///     .set_group_status()
    ///     .group("developers")
    ///     .status(GroupStatus::Enabled)
    ///     .send()
    ///     .await?;
    /// println!("Group enabled");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Disabling a group prevents members from using group-based permissions
    /// - Group configuration (members, policies) is preserved when disabled
    /// - Useful for temporarily suspending group permissions without deleting the group
    pub fn set_group_status(&self) -> SetGroupStatusBldr {
        SetGroupStatus::builder().client(self.clone())
    }
}
