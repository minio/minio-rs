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
use crate::madmin::response::SiteReplicationStatusResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the SiteReplicationStatus admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::site_replication_status`](crate::madmin::madmin_client::MadminClient::site_replication_status) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SiteReplicationStatus {
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
    #[builder(default, setter(doc = "Include buckets in status", strip_option))]
    buckets: Option<bool>,
    #[builder(default, setter(doc = "Include policies in status", strip_option))]
    policies: Option<bool>,
    #[builder(default, setter(doc = "Include users in status", strip_option))]
    users: Option<bool>,
    #[builder(default, setter(doc = "Include groups in status", strip_option))]
    groups: Option<bool>,
    #[builder(
        default,
        setter(doc = "Include ILM expiry rules in status", strip_option)
    )]
    ilm_expiry_rules: Option<bool>,
}

/// Builder type for [`SiteReplicationStatus`].
pub type SiteReplicationStatusBldr =
    SiteReplicationStatusBuilder<((MadminClient,), (), (), (), (), (), (), ())>;

impl ToMadminRequest for SiteReplicationStatus {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        if let Some(true) = self.buckets {
            query_params.add("buckets", "true");
        }
        if let Some(true) = self.policies {
            query_params.add("policies", "true");
        }
        if let Some(true) = self.users {
            query_params.add("users", "true");
        }
        if let Some(true) = self.groups {
            query_params.add("groups", "true");
        }
        if let Some(true) = self.ilm_expiry_rules {
            query_params.add("ilm-expiry-rules", "true");
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/site-replication/status")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for SiteReplicationStatus {
    type MadminResponse = SiteReplicationStatusResponse;
}
