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

use crate::madmin::builders::{ExportBucketMetadata, ExportBucketMetadataBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MadminClient {
    /// Export bucket metadata for backup or migration.
    ///
    /// Returns the bucket's configuration metadata including policies, tags,
    /// lifecycle rules, notifications, encryption settings, and more.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to export metadata from
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use std::fs::File;
    /// use std::io::Write;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = MadminClient::new("http://localhost:9000", "minioadmin", "minioadmin")?;
    ///
    ///     let resp = client
    ///         .export_bucket_metadata("mybucket")?
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     // Save exported metadata to file
    ///     let mut file = File::create("mybucket-metadata.zip")?;
    ///     file.write_all(&resp.data)?;
    ///
    ///     println!("Exported {} bytes of metadata", resp.data.len());
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn export_bucket_metadata<B>(
        &self,
        bucket: B,
    ) -> Result<ExportBucketMetadataBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(ExportBucketMetadata::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
