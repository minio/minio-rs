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

use crate::madmin::builders::{ListAccessKeysBulk, ListAccessKeysBulkBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Lists access keys (service accounts and STS keys) for multiple users.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure the search parameters and send the request.
    /// The response contains a map of usernames to their access keys.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000".parse().unwrap();
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let madmin_client = MadminClient::new(base_url, Some(provider));
    ///
    ///     let response = madmin_client
    ///         .list_access_keys_bulk()
    ///         .users(vec!["user1".to_string(), "user2".to_string()])
    ///         .all(true)
    ///         .build()
    ///         .send()
    ///         .await
    ///         .unwrap();
    ///
    ///     for (username, keys) in response.users_keys {
    ///         println!("User: {}", username);
    ///         if let Some(ref sa) = keys.service_accounts {
    ///             println!("  Service Accounts: {}", sa.len());
    ///         }
    ///         if let Some(ref sts) = keys.sts_keys {
    ///             println!("  STS Keys: {}", sts.len());
    ///         }
    ///     }
    /// }
    /// ```
    pub fn list_access_keys_bulk(&self) -> ListAccessKeysBulkBldr {
        ListAccessKeysBulk::builder().client(self.clone())
    }
}
