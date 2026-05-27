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

use crate::madmin::builders::{ClientPerf, ClientPerfBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Test client-to-server network throughput
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let result = client
    ///     .client_perf()
    ///     .duration(Duration::from_secs(10))
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// println!("Bytes sent: {}, Time: {}ns", result.bytes_send, result.time_spent);
    /// # Ok(())
    /// # }
    /// ```
    pub fn client_perf(&self) -> ClientPerfBldr {
        ClientPerf::builder().client(self.clone())
    }
}
