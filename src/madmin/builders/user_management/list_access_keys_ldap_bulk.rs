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
use crate::madmin::response::ListAccessKeysLDAPBulkResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the List Access Keys LDAP Bulk admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::list_access_keys_ldap_bulk`](crate::madmin::madmin_client::MadminClient::list_access_keys_ldap_bulk) method.
///
/// Lists access keys for LDAP users in bulk.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ListAccessKeysLDAPBulk {
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
    #[builder(
        default,
        setter(into, doc = "List of LDAP user DNs to query access keys for")
    )]
    user_dns: Vec<String>,
    #[builder(
        default,
        setter(
            into,
            doc = "Filter type for access keys (e.g., 'all', 'users-only', 'sts-only', 'svcacc-only')"
        )
    )]
    list_type: Option<String>,
    #[builder(default, setter(doc = "Include all LDAP users when true"))]
    all: bool,
}

/// Builder type for [`ListAccessKeysLDAPBulk`].
pub type ListAccessKeysLDAPBulkBldr =
    ListAccessKeysLDAPBulkBuilder<((MadminClient,), (), (), (), (), ())>;

impl ToMadminRequest for ListAccessKeysLDAPBulk {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        // Validate that both user_dns and all are not specified together
        if !self.user_dns.is_empty() && self.all {
            return Err(Error::Validation(ValidationErr::StrError {
                message: "cannot specify both userDNs and all=true".to_string(),
                source: None,
            }));
        }

        let mut query_params = self.extra_query_params.unwrap_or_default();

        // Add userDNs as query parameters
        for user_dn in &self.user_dns {
            query_params.add("userDNs", user_dn);
        }

        // Add list_type if provided
        if let Some(list_type) = &self.list_type {
            query_params.add("listType", list_type);
        }

        // Add all flag if true
        if self.all {
            query_params.add("all", "true");
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/idp/ldap/list-access-keys-bulk")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .build())
    }
}

impl MadminApi for ListAccessKeysLDAPBulk {
    type MadminResponse = ListAccessKeysLDAPBulkResponse;
}
