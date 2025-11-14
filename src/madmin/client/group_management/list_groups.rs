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

use crate::madmin::builders::{ListGroups, ListGroupsBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Lists all groups on the MinIO server.
    ///
    /// This returns a list of all group names configured on the server.
    /// Groups are used to organize users and simplify permission management.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains a list of group names.
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
    /// let groups = madmin.list_groups().send().await?;
    /// println!("Found {} groups:", groups.len());
    /// for group in groups {
    ///     println!("  - {}", group);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Returns an empty list if no groups are configured
    /// - Groups simplify permission management by allowing policies to be attached to groups
    pub fn list_groups(&self) -> ListGroupsBldr {
        ListGroups::builder().client(self.clone())
    }
}
