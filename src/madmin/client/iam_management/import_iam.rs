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

use crate::madmin::builders::ImportIAM;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Import IAM configuration data into the MinIO server.
    ///
    /// This method imports Identity and Access Management (IAM) configuration
    /// previously exported using `export_iam()`. This is useful for:
    /// - Restoring IAM configuration from backups
    /// - Migrating IAM settings between MinIO deployments
    /// - Disaster recovery scenarios
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
    ///         .import_iam(iam_data)
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Import successful: {}", response.success);
    ///     Ok(())
    /// }
    /// ```
    pub fn import_iam(&self, data: Vec<u8>) -> ImportIAM {
        ImportIAM::builder().client(self.clone()).data(data).build()
    }
}
