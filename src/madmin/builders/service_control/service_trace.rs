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
use crate::madmin::response::ServiceTraceResponse;
use crate::madmin::types::trace::ServiceTraceOpts;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the Service Trace admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::service_trace`](crate::madmin::madmin_client::MadminClient::service_trace) method.
///
/// Service tracing enables real-time monitoring of MinIO server operations by streaming
/// trace events from all nodes in the cluster. Traces can include HTTP requests, internal
/// operations, storage layer activity, and more.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ServiceTrace {
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
        setter(doc = "Trace options specifying which types of events to trace")
    )]
    opts: ServiceTraceOpts,
}

/// Builder type for [`ServiceTrace`].
pub type ServiceTraceBldr = ServiceTraceBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for ServiceTrace {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        for (key, value) in self.opts.to_query_params() {
            query_params.add(&key, &value);
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/trace")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for ServiceTrace {
    type MadminResponse = ServiceTraceResponse;
}
