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

use crate::madmin::builders::{GetBucketQuota, GetBucketQuotaBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MadminClient {
    /// Retrieves the quota configuration for a bucket.
    ///
    /// This returns the current quota limits set on a bucket, including size, rate, and request limits.
    /// If no quota is configured, all values will be 0.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket to get quota for
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains the bucket quota configuration.
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
    /// let quota = madmin.get_bucket_quota("my-bucket")?.send().await?;
    ///
    /// if quota.is_disabled() {
    ///     println!("No quota configured for bucket");
    /// } else {
    ///     println!("Bucket quota: {} bytes", quota.size);
    ///     if quota.rate > 0 {
    ///         println!("Rate limit: {} bytes/sec", quota.rate);
    ///     }
    ///     if quota.requests > 0 {
    ///         println!("Request limit: {} requests", quota.requests);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Returns a BucketQuota with all values set to 0 if no quota is configured
    /// - Quota type is always "hard" for enforcement
    pub fn get_bucket_quota<B>(&self, bucket: B) -> Result<GetBucketQuotaBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(GetBucketQuota::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
