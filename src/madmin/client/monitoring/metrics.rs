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

use crate::madmin::builders::{Metrics, MetricsBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Retrieve Prometheus metrics from MinIO
    ///
    /// Returns metrics in Prometheus format that can be scraped by monitoring systems.
    /// Use the builder methods to select which metrics to include.
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
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MadminClient::new("http://localhost:9000".parse().unwrap(), Some(provider));
    ///
    ///     let metrics = client.metrics()
    ///         .cluster(true)
    ///         .disk(true)
    ///         .send()
    ///         .await
    ///         .expect("Failed to get metrics");
    ///
    ///     println!("Metrics:\n{}", metrics.metrics);
    /// }
    /// ```
    pub fn metrics(&self) -> MetricsBldr {
        Metrics::builder().client(self.clone())
    }
}
