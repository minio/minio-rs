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

use crate::madmin::builders::{InfoAccessKey, InfoAccessKeyBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::typed_parameters::AccessKey;
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Get information about an access key
    ///
    /// Returns detailed information about an access key including its type,
    /// provider, parent user, and associated policies.
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
    /// let base_url = "play.min.io".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("access_key", "secret_key", None);
    /// let client = MadminClient::new(base_url, Some(provider));
    ///
    /// let response = client.info_access_key("some_access_key")?
    ///     .send()
    ///     .await?;
    ///
    /// println!("Access Key: {}", response.info.access_key);
    /// println!("User Type: {}", response.info.user_type);
    /// println!("Status: {}", response.info.account_status);
    /// # Ok(())
    /// # }
    /// ```
    pub fn info_access_key<A>(&self, access_key: A) -> Result<InfoAccessKeyBldr, ValidationErr>
    where
        A: TryInto<AccessKey>,
        A::Error: Into<ValidationErr>,
    {
        Ok(InfoAccessKey::builder()
            .client(self.clone())
            .access_key(access_key.try_into().map_err(Into::into)?))
    }
}
