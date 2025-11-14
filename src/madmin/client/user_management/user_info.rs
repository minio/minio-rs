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

use crate::madmin::builders::{UserInfo, UserInfoBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get information about a specific user on the MinIO server
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("admin_access_key", "admin_secret_key", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let response = client
    ///     .user_info()
    ///     .access_key("username")
    ///     .send()
    ///     .await?;
    /// println!("User status: {}", response.status);
    /// if let Some(policy) = response.policy_name {
    ///     println!("Policy: {}", policy);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn user_info(&self) -> UserInfoBldr {
        UserInfo::builder().client(self.clone())
    }
}
