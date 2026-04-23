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
use crate::madmin::response::SiteReplicationPerfResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use std::time::Duration;
use typed_builder::TypedBuilder;

/// Argument builder for the SiteReplicationPerf admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::site_replication_perf`](crate::madmin::madmin_client::MadminClient::site_replication_perf) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SiteReplicationPerf {
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
    #[builder(setter(doc = "Duration of the site replication performance test"))]
    duration: Duration,
}

/// Builder type for [`SiteReplicationPerf`].
pub type SiteReplicationPerfBldr = SiteReplicationPerfBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for SiteReplicationPerf {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("duration", format!("{}s", self.duration.as_secs()));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/speedtest/site")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for SiteReplicationPerf {
    type MadminResponse = SiteReplicationPerfResponse;
}
