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

use crate::madmin::builders::{KmsApis, KmsApisBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Get list of available KMS APIs
    ///
    /// Retrieves the list of APIs supported by the KMS server.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "localhost:9000".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("admin", "password", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let apis = client.kms_apis().send().await?;
    ///
    /// for api in apis.apis {
    ///     println!("API: {} - {}", api.method, api.path);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn kms_apis(&self) -> KmsApisBldr {
        KmsApis::builder().client(self.clone())
    }
}
