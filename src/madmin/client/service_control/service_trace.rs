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

use crate::madmin::builders::{ServiceTrace, ServiceTraceBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Stream real-time trace events from the MinIO server
    ///
    /// This method enables monitoring of various server operations in real-time.
    /// You can filter which types of events to trace using [`ServiceTraceOpts`](crate::madmin::types::trace::ServiceTraceOpts).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::trace::ServiceTraceOpts;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use futures_util::StreamExt;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// // Configure trace options to only trace S3 API calls
    /// let opts = ServiceTraceOpts {
    ///     s3: Some(true),
    ///     only_errors: Some(false),
    ///     ..Default::default()
    /// };
    ///
    /// // Start tracing and iterate over events
    /// let mut trace_stream = client
    ///     .service_trace()
    ///     .opts(opts)
    ///     .send()
    ///     .await?
    ///     .into_stream();
    ///
    /// // Process trace events as they arrive
    /// while let Some(result) = trace_stream.next().await {
    ///     match result {
    ///         Ok(trace_info) => {
    ///             println!("Trace: {} on {}",
    ///                 trace_info.trace.func_name,
    ///                 trace_info.trace.node_name
    ///             );
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn service_trace(&self) -> ServiceTraceBldr {
        ServiceTrace::builder().client(self.clone())
    }
}
