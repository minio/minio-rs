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

use crate::madmin::builders::{RevokeTokens, RevokeTokensBldr};
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    /// Revoke authentication tokens for builtin users.
    ///
    /// # Arguments
    ///
    /// Returns a builder for the [`RevokeTokens`] operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::madmin::madmin_client::MadminClient;
    /// use minio::madmin::types::MadminApi;
    /// use minio::madmin::types::user::{RevokeTokensReq, TokenRevokeType};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let base_url: BaseUrl = "http://localhost:9000".parse()?;
    ///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let madmin_client = MadminClient::new(base_url, Some(provider));
    ///
    ///     let req = RevokeTokensReq {
    ///         user: "test-user".to_string(),
    ///         token_revoke_type: TokenRevokeType::Sts,
    ///         full_revoke: true,
    ///     };
    ///
    ///     madmin_client
    ///         .revoke_tokens()
    ///         .request(req)
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     println!("Tokens revoked successfully");
    ///     Ok(())
    /// }
    /// ```
    pub fn revoke_tokens(&self) -> RevokeTokensBldr {
        RevokeTokens::builder().client(self.clone())
    }
}
