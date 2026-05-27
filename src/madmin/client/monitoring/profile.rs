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

use crate::madmin::builders::monitoring::{Profile, ProfileBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Start profiling and download profiling data.
    ///
    /// This combines starting a profiling session and downloading the results.
    /// The profiling data is returned as a ZIP archive containing profile data
    /// from all nodes in the cluster.
    ///
    /// # Arguments
    ///
    /// * `profiler_type` - Type of profiler (CPU, Memory, Block, etc.)
    /// * `duration` - How long to run the profiler
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
    /// use minio::madmin::types::profiling::ProfilerType;
    /// use minio::s3::creds::StaticProvider;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Profile CPU for 30 seconds
    /// let profile_data = madmin.profile()
    ///     .profiler_type(ProfilerType::CPU)
    ///     .duration(Duration::from_secs(30))
    ///     .send()
    ///     .await?;
    ///
    /// // Save to file
    /// std::fs::write("cpu-profile.zip", &*profile_data)?;
    ///
    /// // Profile memory for 10 seconds
    /// let mem_profile = madmin.profile()
    ///     .profiler_type(ProfilerType::MEM)
    ///     .duration(Duration::from_secs(10))
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Available Profiler Types
    ///
    /// - `ProfilerType::CPU` - CPU usage profiling
    /// - `ProfilerType::CPUIO` - CPU I/O profiling (fgprof)
    /// - `ProfilerType::MEM` - Memory allocation profiling
    /// - `ProfilerType::Block` - Blocking operations profiling
    /// - `ProfilerType::Mutex` - Mutex contention profiling
    /// - `ProfilerType::Trace` - Execution trace
    /// - `ProfilerType::Threads` - Thread profiling
    /// - `ProfilerType::Goroutines` - Goroutine profiling
    /// - `ProfilerType::Runtime` - Runtime statistics
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Profiling impacts server performance during collection
    /// - Returns ZIP archive with profiling data from all cluster nodes
    /// - Typical durations: 10-60 seconds for CPU, 5-30 seconds for memory
    /// - Use Go's pprof tool to analyze the data
    pub fn profile(&self) -> ProfileBldr {
        Profile::builder().client(self.clone())
    }
}
