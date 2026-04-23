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
use crate::madmin::response::MetricsResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for retrieving Prometheus metrics from MinIO.
///
/// This struct constructs the parameters required for the [`MadminClient::metrics`](crate::madmin::madmin_client::MadminClient::metrics) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct Metrics {
    #[builder(!default)]
    client: MadminClient,
    /// Disk
    #[builder(default = false)]
    disk: bool,
    /// Cluster
    #[builder(default = false)]
    cluster: bool,
    /// Name of the bucket
    #[builder(default = false)]
    bucket: bool,
    /// Resource
    #[builder(default = false)]
    resource: bool,
    /// Debug
    #[builder(default = false)]
    debug: bool,
}

/// Builder type for [`Metrics`].
pub type MetricsBldr = MetricsBuilder<((MadminClient,), (), (), (), (), ())>;

impl ToMadminRequest for Metrics {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = Multimap::new();

        if self.disk {
            query_params.add("disk", "true");
        }
        if self.cluster {
            query_params.add("cluster", "true");
        }
        if self.bucket {
            query_params.add("bucket", "true");
        }
        if self.resource {
            query_params.add("resource", "true");
        }
        if self.debug {
            query_params.add("debug", "true");
        }

        let headers = Multimap::new();

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/prometheus/metrics")
            .api_version(4)
            .query_params(query_params)
            .headers(headers)
            .build())
    }
}

impl MadminApi for Metrics {
    type MadminResponse = MetricsResponse;
}
