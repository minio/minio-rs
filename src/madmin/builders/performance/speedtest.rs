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
use crate::madmin::response::SpeedtestResponse;
use crate::madmin::types::performance::SpeedtestOpts;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the Speedtest admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::speedtest`](crate::madmin::madmin_client::MadminClient::speedtest) method.
///
/// Speedtest performs object read/write performance tests on the MinIO cluster,
/// measuring throughput and latency statistics across all nodes.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct Speedtest {
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
        setter(doc = "Speed test options specifying size, concurrency, duration, etc.")
    )]
    opts: SpeedtestOpts,
}

/// Builder type for [`Speedtest`].
pub type SpeedtestBldr = SpeedtestBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for Speedtest {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        for (key, value) in self.opts.to_query_params() {
            query_params.add(&key, &value);
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/speedtest")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for Speedtest {
    type MadminResponse = SpeedtestResponse;
}
