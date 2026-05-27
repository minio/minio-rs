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

use crate::madmin::builders::ImportIAMV2;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Import IAM configuration data into the MinIO server with detailed feedback (v2).
    ///
    /// This is an enhanced version of `import_iam()` that provides detailed feedback
    /// about the import operation, including which entities were:
    /// - **Added**: Successfully imported new entities
    /// - **Skipped**: Already exist and were not modified
    /// - **Removed**: Deleted as part of the import
    /// - **Failed**: Encountered errors during import
    ///
    /// # Arguments
    ///
    /// * `data` - The IAM export data (typically JSON format)
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to execute the import operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MadminClient::new("http://localhost:9000".parse()?, Some(provider));
    ///
    ///     // Read previously exported IAM data
    ///     let iam_data = std::fs::read("iam-backup.json")?;
    ///
    ///     let response = client
    ///         .import_iam_v2(iam_data)
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Import Results:");
    ///     println!("  Added: {:?}", response.result.added);
    ///     println!("  Skipped: {:?}", response.result.skipped);
    ///     println!("  Removed: {:?}", response.result.removed);
    ///     if !response.result.failed.policies.as_ref().map_or(true, |p| p.is_empty()) {
    ///         println!("  Failed: {:?}", response.result.failed);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn import_iam_v2(&self, data: Vec<u8>) -> ImportIAMV2 {
        ImportIAMV2::builder()
            .client(self.clone())
            .data(data)
            .build()
    }
}
