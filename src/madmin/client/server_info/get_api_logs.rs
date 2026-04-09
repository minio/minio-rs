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

use crate::madmin::builders::{GetAPILogs, GetAPILogsBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Creates a builder for fetching API logs.
    ///
    /// **Note:** This API returns MessagePack-encoded streaming data.
    /// Full decoding support requires the `rmp-serde` crate.
    /// Current implementation returns raw response bytes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::types::api_logs::APILogOpts;
    /// # async fn example(client: minio::madmin::madmin_client::MadminClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let opts = APILogOpts {
    ///     api_name: Some("PutObject".to_string()),
    ///     status_code: Some(200),
    ///     ..Default::default()
    /// };
    ///
    /// let response = client
    ///     .get_api_logs()
    ///     .opts(opts)
    ///     .send()
    ///     .await?;
    ///
    /// // response.data contains MessagePack-encoded log entries
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_api_logs(&self) -> GetAPILogsBldr {
        GetAPILogs::builder().client(self.clone())
    }
}
