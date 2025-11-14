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

use crate::madmin::builders::{SetUser, SetUserBldr};
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::typed_parameters::{AccessKey, SecretKey};
use crate::s3::error::ValidationErr;

impl MadminClient {
    /// Create or update a user
    ///
    /// This operation creates a new user or updates an existing user's credentials,
    /// status, and optionally their IAM policy.
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
    /// // Create a new user
    /// client.set_user("newuser", "newpassword")?
    ///     .send()
    ///     .await?;
    ///
    /// // Update user with disabled status
    /// client.set_user("newuser", "newpassword")?
    ///     .status("disabled")
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_user<A, S>(&self, access_key: A, secret_key: S) -> Result<SetUserBldr, ValidationErr>
    where
        A: TryInto<AccessKey>,
        A::Error: Into<ValidationErr>,
        S: TryInto<SecretKey>,
        S::Error: Into<ValidationErr>,
    {
        Ok(SetUser::builder()
            .client(self.clone())
            .access_key(access_key.try_into().map_err(Into::into)?)
            .secret_key(secret_key.try_into().map_err(Into::into)?))
    }
}
