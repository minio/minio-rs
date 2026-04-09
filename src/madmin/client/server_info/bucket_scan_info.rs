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

use crate::madmin::builders::{BucketScanInfo, BucketScanInfoBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MadminClient {
    /// Get bucket scanning status
    ///
    /// Returns scanning information for all pools and sets associated with the specified bucket,
    /// including scan cycle numbers, ongoing status, and completion timestamps.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to get scan information for
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::types::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url: BaseUrl = "http://localhost:9000".parse()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let scan_info = client
    ///     .bucket_scan_info("my-bucket")?
    ///     .send()
    ///     .await?;
    ///
    /// for scan in &scan_info.scans {
    ///     println!("Pool {}, Set {}: Cycle {}, Ongoing: {}",
    ///         scan.pool, scan.set, scan.cycle, scan.ongoing);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn bucket_scan_info<B>(&self, bucket: B) -> Result<BucketScanInfoBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(BucketScanInfo::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
