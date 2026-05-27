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

use crate::madmin::builders::{BucketReplicationMRF, BucketReplicationMRFBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MadminClient {
    /// Get MRF (Metadata Replication Framework) backlog for a bucket.
    ///
    /// Returns information about objects that failed to replicate and are in the MRF queue
    /// for retry. This is useful for troubleshooting replication failures and monitoring
    /// replication health.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to check MRF backlog
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to configure additional options and send the request.
    /// The response contains a list of MRF backlog entries.
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
    /// // Get MRF backlog for all nodes
    /// let mrf_resp = madmin.bucket_replication_mrf("mybucket")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Found {} objects in MRF backlog", mrf_resp.entries.len());
    /// for entry in mrf_resp.entries {
    ///     println!(
    ///         "Object: {}, Node: {}, Retries: {}, Error: {:?}",
    ///         entry.object, entry.node_name, entry.retry_count, entry.err
    ///     );
    /// }
    ///
    /// // Get MRF backlog for specific node
    /// let mrf_resp = madmin.bucket_replication_mrf("mybucket")?
    ///     .node("node1".to_string())
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
    /// - MRF = Metadata Replication Framework
    /// - Returns stream of MRF entries (collected into Vec)
    /// - Objects in MRF have failed replication and are being retried
    /// - Use node parameter to filter by specific cluster node
    pub fn bucket_replication_mrf<B>(
        &self,
        bucket: B,
    ) -> Result<BucketReplicationMRFBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(BucketReplicationMRF::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
