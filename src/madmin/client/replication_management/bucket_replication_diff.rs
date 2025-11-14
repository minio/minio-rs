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

use crate::madmin::builders::{BucketReplicationDiff, BucketReplicationDiffBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MadminClient {
    /// Get replication diff for non-replicated entries.
    ///
    /// Returns information about objects that have not been replicated or have replication pending.
    /// This is useful for troubleshooting replication issues and understanding replication lag.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to check
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains a list of diff information for unreplicated objects.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::replication::ReplDiffOpts;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let madmin = MadminClient::new("http://localhost:9000", Some(provider));
    ///
    /// // Get replication diff for all objects
    /// let diff_resp = madmin.bucket_replication_diff("mybucket")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Found {} unreplicated objects", diff_resp.diffs.len());
    /// for diff in diff_resp.diffs {
    ///     println!("Object: {}, Status: {:?}", diff.object, diff.replication_status);
    /// }
    ///
    /// // Get replication diff with options
    /// let opts = ReplDiffOpts {
    ///     arn: Some("arn:minio:replication::target1".to_string()),
    ///     verbose: true,
    ///     prefix: Some("documents/".to_string()),
    /// };
    ///
    /// let diff_resp = madmin.bucket_replication_diff("mybucket")?
    ///     .opts(opts)
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Notes
    ///
    /// - Requires admin credentials
    /// - Bucket must have replication configured
    /// - Returns stream of diff information (collected into Vec)
    /// - Use ARN option to filter by specific replication target
    /// - Use prefix option to check specific object prefix
    pub fn bucket_replication_diff<B>(
        &self,
        bucket: B,
    ) -> Result<BucketReplicationDiffBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(BucketReplicationDiff::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
