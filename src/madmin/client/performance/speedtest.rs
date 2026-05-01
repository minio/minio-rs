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

use crate::madmin::builders::{Speedtest, SpeedtestBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Run object read/write performance tests on the MinIO cluster
    ///
    /// This method performs throughput and latency measurements for PUT and GET operations,
    /// providing detailed statistics across all nodes in the cluster.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::performance::SpeedtestOpts;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use futures_util::StreamExt;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let opts = SpeedtestOpts {
    ///     size: Some(1024 * 1024), // 1 MB objects
    ///     concurrency: Some(10),
    ///     duration: Some(Duration::from_secs(30)),
    ///     ..Default::default()
    /// };
    ///
    /// let mut speedtest_stream = client
    ///     .speedtest()
    ///     .opts(opts)
    ///     .build()
    ///     .send()
    ///     .await?
    ///     .into_stream();
    ///
    /// while let Some(result) = speedtest_stream.next().await {
    ///     match result {
    ///         Ok(test_result) => {
    ///             println!("PUT: {} bytes/sec", test_result.put_stats.throughput_per_sec);
    ///             println!("GET: {} bytes/sec", test_result.get_stats.throughput_per_sec);
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn speedtest(&self) -> SpeedtestBldr {
        Speedtest::builder().client(self.clone())
    }
}
