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

use crate::madmin::builders::ExportIAM;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Export all IAM configuration data from the MinIO server.
    ///
    /// This method exports all Identity and Access Management (IAM) configuration
    /// including users, policies, groups, service accounts, and policy mappings.
    /// The exported data can be used for backup, migration, or disaster recovery.
    ///
    /// # Returns
    ///
    /// Returns a builder that can be used to execute the export operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::s3::creds::StaticProvider;
    /// use std::fs::File;
    /// use std::io::Write;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MadminClient::new("http://localhost:9000".parse()?, Some(provider));
    ///
    ///     let response = client
    ///         .export_iam()
    ///         .send()
    ///         .await?;
    ///
    ///     // Save to file
    ///     let mut file = File::create("iam-backup.json")?;
    ///     file.write_all(&response.data)?;
    ///
    ///     println!("Exported {} bytes of IAM data", response.data.len());
    ///     Ok(())
    /// }
    /// ```
    pub fn export_iam(&self) -> ExportIAM {
        ExportIAM::builder().client(self.clone()).build()
    }
}
