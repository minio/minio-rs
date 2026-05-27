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

use crate::madmin::builders::{TopLocks, TopLocksBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get the top locks currently held on the MinIO server
    ///
    /// Returns the oldest locks currently active on the server. Useful for debugging
    /// lock contention and identifying potentially stuck operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("access_key", "secret_key", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// // Get the top 10 locks (default)
    /// let response = client.top_locks().send().await?;
    /// for lock in response.locks {
    ///     println!("Lock on {}: {} ({})", lock.resource, lock.lock_type, lock.source);
    /// }
    ///
    /// // Get the top 20 locks, including stale locks
    /// let response = client.top_locks().count(20).stale(true).send().await?;
    /// println!("Found {} locks", response.locks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn top_locks(&self) -> TopLocksBldr {
        TopLocks::builder().client(self.clone())
    }
}
