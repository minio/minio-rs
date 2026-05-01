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
use crate::madmin::response::DetachPolicyLDAPResponse;
use crate::madmin::types::policy::PolicyAssociationReq;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the DetachPolicyLDAP admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::detach_policy_ldap`](crate::madmin::madmin_client::MadminClient::detach_policy_ldap) method.
///
/// Detaches policies from LDAP users or groups.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct DetachPolicyLDAP {
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
    #[builder(default, setter(into, doc = "Array of policy names to detach"))]
    policies: Vec<String>,
    #[builder(
        default,
        setter(
            strip_option,
            into,
            doc = "LDAP user DN to detach policies from (mutually exclusive with group)"
        )
    )]
    user: Option<String>,
    #[builder(
        default,
        setter(
            strip_option,
            into,
            doc = "LDAP group DN to detach policies from (mutually exclusive with user)"
        )
    )]
    group: Option<String>,
    #[builder(
        default,
        setter(strip_option, into, doc = "Optional LDAP configuration name")
    )]
    config_name: Option<String>,
}

/// Builder type for [`DetachPolicyLDAP`].
pub type DetachPolicyLDAPBldr = DetachPolicyLDAPBuilder<((MadminClient,), (), (), (), (), (), ())>;

impl ToMadminRequest for DetachPolicyLDAP {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let req = PolicyAssociationReq {
            policies: self.policies,
            user: self.user,
            group: self.group,
            config_name: self.config_name,
        };

        req.validate().map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: e,
                source: None,
            })
        })?;

        let body_bytes =
            serde_json::to_vec(&req).map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/idp/ldap/policy/detach")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(Arc::new(SegmentedBytes::from(Bytes::from(
                body_bytes,
            )))))
            .api_version(4)
            .build())
    }
}

impl MadminApi for DetachPolicyLDAP {
    type MadminResponse = DetachPolicyLDAPResponse;
}
