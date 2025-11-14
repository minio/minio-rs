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

use crate::madmin::builders::{ListConfigHistoryKV, ListConfigHistoryKVBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// List configuration history entries
    ///
    /// Returns a list of configuration history entries sorted by creation time.
    /// Each entry contains a restore ID that can be used to restore that configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url: BaseUrl = "http://localhost:9000".parse()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let madmin_client = MadminClient::new(base_url, Some(provider));
    ///
    ///     // List last 20 configuration history entries
    ///     let response = madmin_client
    ///         .list_config_history_kv()
    ///         .count(20)
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     for entry in response.entries() {
    ///         println!("Restore ID: {}, Created: {}",
    ///                  entry.restore_id, entry.create_time);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn list_config_history_kv(&self) -> ListConfigHistoryKVBldr {
        ListConfigHistoryKV::builder().client(self.clone())
    }
}
