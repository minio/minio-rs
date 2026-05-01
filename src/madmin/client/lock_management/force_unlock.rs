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

use crate::madmin::builders::lock_management::ForceUnlock;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Forcibly release locks on specified paths.
    ///
    /// This operation should be used with caution as it can potentially cause
    /// data inconsistencies if locks are released while operations are in progress.
    ///
    /// # Arguments
    ///
    /// * `paths` - Vector of paths to unlock (e.g., ["mybucket/myobject"])
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::client::Client;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = Client::new(base_url, Some(Box::new(provider)), None, None)?;
    ///     let madmin = client.madmin();
    ///
    ///     // Check for stuck locks
    ///     let locks = madmin.top_locks().send().await?;
    ///     let stuck_paths: Vec<String> = locks
    ///         .iter()
    ///         .filter(|lock| lock.elapsed > 300_000_000_000) // > 5 minutes in nanoseconds
    ///         .map(|lock| lock.resource.clone())
    ///         .collect();
    ///
    ///     if !stuck_paths.is_empty() {
    ///         println!("Forcing unlock of {} stuck locks", stuck_paths.len());
    ///         madmin.force_unlock(stuck_paths).send().await?;
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn force_unlock(&self, paths: Vec<String>) -> ForceUnlock {
        ForceUnlock::builder()
            .client(self.clone())
            .paths(paths)
            .build()
    }
}
