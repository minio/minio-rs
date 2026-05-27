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

use crate::madmin::builders::{RestoreConfigHistoryKV, RestoreConfigHistoryKVBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Restore a previous configuration from history
    ///
    /// Reverts the server configuration to a previously saved state identified by restore ID.
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
    ///     // Restore configuration using a restore ID from history
    ///     madmin_client
    ///         .restore_config_history_kv()
    ///         .restore_id("restore-id-123")
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Configuration restored successfully");
    ///     Ok(())
    /// }
    /// ```
    pub fn restore_config_history_kv(&self) -> RestoreConfigHistoryKVBldr {
        RestoreConfigHistoryKV::builder().client(self.clone())
    }
}
