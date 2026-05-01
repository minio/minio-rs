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

use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::RevokeTokensLDAPResponse;
use crate::madmin::types::user::{RevokeTokensReq, TokenRevokeType};
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the RevokeTokensLDAP admin API operation.
///
/// Revokes authentication tokens for LDAP-authenticated users.
///
/// This struct constructs the parameters required for the [`MadminClient::revoke_tokens_ldap`] method.
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
///         user: "cn=testuser,ou=users,dc=example,dc=com".to_string(),
///         token_revoke_type: TokenRevokeType::Sts,
///         full_revoke: true,
///     };
///
///     madmin_client
///         .revoke_tokens_ldap()
///         .request(req)
///         .build()
///         .send()
///         .await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct RevokeTokensLDAP {
    #[builder(!default)]
    client: MadminClient,
    #[builder(
        default,
        setter(into, doc = "Optional extra HTTP headers to include in the request")
    )]
    extra_headers: Option<Multimap>,
    #[builder(
        default,
        setter(
            into,
            doc = "Optional extra query parameters to include in the request"
        )
    )]
    extra_query_params: Option<Multimap>,
    #[builder(setter(doc = "Token revocation request for LDAP user"))]
    request: RevokeTokensReq,
}

pub type RevokeTokensLDAPBldr = RevokeTokensLDAPBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for RevokeTokensLDAP {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("user", &self.request.user);

        let token_type = match self.request.token_revoke_type {
            TokenRevokeType::All => "all",
            TokenRevokeType::Sts => "sts",
            TokenRevokeType::ServiceAccount => "serviceaccount",
        };
        query_params.add("tokenRevokeType", token_type);

        if self.request.full_revoke {
            query_params.add("fullRevoke", "true");
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/revoke-tokens/ldap")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .build())
    }
}

impl MadminApi for RevokeTokensLDAP {
    type MadminResponse = RevokeTokensLDAPResponse;
}
