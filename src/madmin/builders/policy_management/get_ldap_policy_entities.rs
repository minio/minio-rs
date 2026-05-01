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
use crate::madmin::response::GetLDAPPolicyEntitiesResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the GetLDAPPolicyEntities admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::get_ldap_policy_entities`](crate::madmin::madmin_client::MadminClient::get_ldap_policy_entities) method.
///
/// Retrieves LDAP policy entity mappings showing relationships between users, groups, and policies.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct GetLDAPPolicyEntities {
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
        setter(into, doc = "Optional list of user DNs to filter results")
    )]
    users: Vec<String>,
    #[builder(
        default,
        setter(into, doc = "Optional list of group DNs to filter results")
    )]
    groups: Vec<String>,
    #[builder(
        default,
        setter(into, doc = "Optional list of policy names to filter results")
    )]
    policy: Vec<String>,
    #[builder(
        default,
        setter(strip_option, into, doc = "Optional LDAP configuration name to query")
    )]
    config_name: Option<String>,
}

/// Builder type for [`GetLDAPPolicyEntities`].
pub type GetLDAPPolicyEntitiesBldr =
    GetLDAPPolicyEntitiesBuilder<((MadminClient,), (), (), (), (), (), ())>;

impl ToMadminRequest for GetLDAPPolicyEntities {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        // Add user query parameters
        for user in &self.users {
            query_params.add("user", user);
        }

        // Add group query parameters
        for group in &self.groups {
            query_params.add("group", group);
        }

        // Add policy query parameters
        for policy in &self.policy {
            query_params.add("policy", policy);
        }

        // Add config name if provided
        if let Some(config_name) = &self.config_name {
            query_params.add("configName", config_name);
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/idp/ldap/policy-entities")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for GetLDAPPolicyEntities {
    type MadminResponse = GetLDAPPolicyEntitiesResponse;
}
