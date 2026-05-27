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

use crate::madmin::builders::{DriveSpeedtest, DriveSpeedtestBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Test read/write performance of drives in the MinIO cluster
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::performance::DriveSpeedTestOpts;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use futures_util::StreamExt;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let opts = DriveSpeedTestOpts {
    ///     serial: Some(false),
    ///     block_size: Some(4 * 1024 * 1024),
    ///     file_size: Some(1024 * 1024 * 1024),
    /// };
    ///
    /// let mut drive_stream = client
    ///     .drive_speedtest()
    ///     .opts(opts)
    ///     .build()
    ///     .send()
    ///     .await?
    ///     .into_stream();
    ///
    /// while let Some(result) = drive_stream.next().await {
    ///     match result {
    ///         Ok(test_result) => {
    ///             println!("Endpoint: {}", test_result.endpoint);
    ///             if let Some(perfs) = test_result.drive_perf {
    ///                 for perf in perfs {
    ///                     println!("  {}: Read={} bytes/s, Write={} bytes/s",
    ///                         perf.path, perf.read_throughput, perf.write_throughput);
    ///                 }
    ///             }
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn drive_speedtest(&self) -> DriveSpeedtestBldr {
        DriveSpeedtest::builder().client(self.clone())
    }
}
