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
use crate::madmin::response::HealResponse;
use crate::madmin::types::heal::HealOpts;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::BucketName;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the Heal admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::heal`](crate::madmin::madmin_client::MadminClient::heal) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct Heal {
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
    #[builder(default, setter(into, doc = "Optional bucket name"))]
    bucket: Option<BucketName>,
    #[builder(default, setter(into, doc = "Optional prefix to filter objects"))]
    prefix: Option<String>,
    #[builder(default, setter(into, doc = "Optional heal operation options"))]
    opts: Option<HealOpts>,
    #[builder(default, setter(into, doc = "Optional client token for tracking"))]
    client_token: Option<String>,
    /// Force start the heal operation
    #[builder(default = false)]
    force_start: bool,
    /// Force stop the heal operation
    #[builder(default = false)]
    force_stop: bool,
}

/// Builder type for [`Heal`].
pub type HealBldr = HealBuilder<((MadminClient,), (), (), (), (), (), (), (), ())>;

impl MadminApi for Heal {
    type MadminResponse = HealResponse;
}

impl ToMadminRequest for Heal {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();

        if let Some(token) = self.client_token {
            query_params.add("clientToken", token);
        }

        if self.force_start {
            query_params.add("forceStart", "true");
        }

        if self.force_stop {
            query_params.add("forceStop", "true");
        }

        let path = match (&self.bucket, &self.prefix) {
            (Some(bucket), Some(prefix)) => format!("/heal/{}/{}", bucket, prefix),
            (Some(bucket), None) => format!("/heal/{}", bucket),
            (None, Some(prefix)) => format!("/heal/{}", prefix),
            (None, None) => "/heal".to_string(),
        };

        let body = if let Some(opts) = self.opts {
            let json_data = serde_json::to_vec(&opts)
                .map_err(|e| Error::Validation(crate::s3::error::ValidationErr::JsonError(e)))?;
            Some(Arc::new(SegmentedBytes::from(Bytes::from(json_data))))
        } else {
            None
        };

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(path)
            .api_version(4)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}
