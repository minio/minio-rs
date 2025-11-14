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
use crate::madmin::response::SiteReplicationAddResponse;
use crate::madmin::types::site_replication::{PeerSite, SRAddOptions};
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the SiteReplicationAdd admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::site_replication_add`](crate::madmin::madmin_client::MadminClient::site_replication_add) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SiteReplicationAdd {
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
    #[builder(setter(doc = "List of peer sites to add for replication"))]
    sites: Vec<PeerSite>,
    #[builder(
        default,
        setter(
            doc = "Optional configuration options for site replication",
            strip_option
        )
    )]
    options: Option<SRAddOptions>,
}

/// Builder type for [`SiteReplicationAdd`].
pub type SiteReplicationAddBldr = SiteReplicationAddBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for SiteReplicationAdd {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        if let Some(opts) = &self.options
            && opts.disable_ilm_expiry_replication
        {
            query_params.add("disableILMExpiryReplication", "true");
        }

        let body_vec = serde_json::to_vec(&self.sites)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(body_vec)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/site-replication/add")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for SiteReplicationAdd {
    type MadminResponse = SiteReplicationAddResponse;
}
