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
use crate::madmin::response::SiteReplicationRemoveResponse;
use crate::madmin::types::site_replication::SRRemoveReq;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the SiteReplicationRemove admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::site_replication_remove`](crate::madmin::madmin_client::MadminClient::site_replication_remove) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SiteReplicationRemove {
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
        setter(doc = "List of site names to remove (omit to remove all)")
    )]
    site_names: Vec<String>,
    #[builder(default = false, setter(doc = "Remove all sites from replication"))]
    remove_all: bool,
}

/// Builder type for [`SiteReplicationRemove`].
pub type SiteReplicationRemoveBldr =
    SiteReplicationRemoveBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for SiteReplicationRemove {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let req = SRRemoveReq {
            requesting_dep_id: String::new(),
            site_names: self.site_names,
            remove_all: self.remove_all,
        };

        let body_vec =
            serde_json::to_vec(&req).map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(body_vec)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/site-replication/remove")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for SiteReplicationRemove {
    type MadminResponse = SiteReplicationRemoveResponse;
}
