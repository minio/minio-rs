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

use crate::madmin::builders::{GetLicenseInfo, GetLicenseInfoBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get MinIO Enterprise license information.
    ///
    /// Returns license details including organization, plan, expiration date, and trial status.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = MadminClient::new("http://localhost:9000", "minioadmin", "minioadmin")?;
    ///
    ///     let resp = client
    ///         .get_license_info()
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Organization: {}", resp.license_info.organization);
    ///     println!("Plan: {}", resp.license_info.plan);
    ///     println!("Expires: {}", resp.license_info.expires_at);
    ///     println!("Trial: {}", resp.license_info.trial);
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn get_license_info(&self) -> GetLicenseInfoBldr {
        GetLicenseInfo::builder().client(self.clone())
    }
}
