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
use crate::madmin::response::SiteReplicationEditResponse;
use crate::madmin::types::site_replication::PeerInfo;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the SiteReplicationEdit admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::site_replication_edit`](crate::madmin::madmin_client::MadminClient::site_replication_edit) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SiteReplicationEdit {
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
    #[builder(setter(doc = "Deployment ID of the site to edit"))]
    deployment_id: String,
    #[builder(setter(doc = "New endpoint for the site"))]
    endpoint: String,
    #[builder(default, setter(doc = "Disable ILM expiry replication", strip_option))]
    disable_ilm_expiry_replication: Option<bool>,
    #[builder(default, setter(doc = "Enable ILM expiry replication", strip_option))]
    enable_ilm_expiry_replication: Option<bool>,
}

/// Builder type for [`SiteReplicationEdit`].
pub type SiteReplicationEditBldr =
    SiteReplicationEditBuilder<((MadminClient,), (), (), (), (), (), ())>;

impl ToMadminRequest for SiteReplicationEdit {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        if let Some(disable) = self.disable_ilm_expiry_replication
            && disable
        {
            query_params.add("disableILMExpiryReplication", "true");
        }

        if let Some(enable) = self.enable_ilm_expiry_replication
            && enable
        {
            query_params.add("enableILMExpiryReplication", "true");
        }

        let peer_info = PeerInfo {
            endpoint: self.endpoint,
            name: String::new(),
            deployment_id: self.deployment_id,
        };

        let body_vec = serde_json::to_vec(&peer_info)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(body_vec)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/site-replication/edit")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for SiteReplicationEdit {
    type MadminResponse = SiteReplicationEditResponse;
}
