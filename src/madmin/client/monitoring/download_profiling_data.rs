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

use crate::madmin::builders::monitoring::{DownloadProfilingData, DownloadProfilingDataBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Download previously collected profiling data.
    ///
    /// Downloads profiling data from the MinIO server that was started by a previous
    /// profiling session (e.g., via StartProfiling or another mechanism). The profiling
    /// data is returned as a ZIP archive containing profile data from all nodes in the cluster.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains binary profiling data as a ZIP archive.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Download profiling data from a previous profiling session
    /// let profile_data = madmin.download_profiling_data()
    ///     .send()
    ///     .await?;
    ///
    /// // Save to file
    /// std::fs::write("profiling-data.zip", &*profile_data.data)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - This downloads data from a profiling session that was previously started
    /// - Use the [`profile()`](MadminClient::profile) method for a combined start+download operation
    /// - Returns ZIP archive with profiling data from all cluster nodes
    /// - Use Go's pprof tool to analyze the data
    pub fn download_profiling_data(&self) -> DownloadProfilingDataBldr {
        DownloadProfilingData::builder().client(self.clone())
    }
}
