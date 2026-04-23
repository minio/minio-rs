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

use crate::madmin::builders::{ImportBucketMetadata, ImportBucketMetadataBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;
use bytes::Bytes;

impl MadminClient {
    /// Import bucket metadata for restoration or migration.
    ///
    /// Restores bucket configuration metadata including policies, tags,
    /// lifecycle rules, notifications, encryption settings, and more.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to import metadata into
    /// * `content` - Metadata content (typically from `export_bucket_metadata`)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = MadminClient::new("http://localhost:9000", "minioadmin", "minioadmin")?;
    ///
    ///     // Read exported metadata
    ///     let content = fs::read("mybucket-metadata.zip")?;
    ///
    ///     let resp = client
    ///         .import_bucket_metadata("mybucket", content.into())?
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     // Check import status
    ///     if let Some(buckets) = resp.result.buckets {
    ///         for (bucket_name, status) in buckets {
    ///             println!("Bucket: {}", bucket_name);
    ///             if status.object_lock.is_set {
    ///                 println!("  Object lock: imported");
    ///             }
    ///             if let Some(err) = status.err {
    ///                 println!("  Error: {}", err);
    ///             }
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn import_bucket_metadata<B>(
        &self,
        bucket: B,
        content: Bytes,
    ) -> Result<ImportBucketMetadataBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(ImportBucketMetadata::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .content(content))
    }
}
