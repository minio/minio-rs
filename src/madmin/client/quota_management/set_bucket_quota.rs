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

use crate::madmin::builders::{SetBucketQuota, SetBucketQuotaBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MadminClient {
    /// Sets the quota configuration for a bucket.
    ///
    /// This configures capacity and usage limits for a bucket, including size, rate, and request limits.
    /// Setting all quota values to 0 disables quota enforcement.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket to set quota for
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::quota::BucketQuota;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Set a 10 GB quota on the bucket
    /// let quota = BucketQuota::new(10 * 1024 * 1024 * 1024)
    ///     .with_rate(1024 * 1024)  // 1 MB/s rate limit
    ///     .with_requests(1000);     // 1000 requests limit
    ///
    /// madmin.set_bucket_quota("my-bucket")?.quota(quota).send().await?;
    /// println!("Bucket quota configured successfully");
    ///
    /// // Disable quota by setting to 0
    /// let no_quota = BucketQuota::new(0);
    /// madmin.set_bucket_quota("my-bucket")?.quota(no_quota).send().await?;
    /// println!("Bucket quota disabled");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Setting quota to 0 disables quota enforcement
    /// - Quota enforcement is always "hard" - operations exceeding quota will fail
    /// - Useful for multi-tenant deployments to prevent resource abuse
    /// - Rate and request limits are optional; set to 0 to disable
    pub fn set_bucket_quota<B>(&self, bucket: B) -> Result<SetBucketQuotaBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(SetBucketQuota::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
