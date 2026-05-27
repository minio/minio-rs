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
use crate::madmin::response::ServiceActionResponse;
use crate::madmin::types::service::ServiceAction as ServiceActionType;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use std::time::Duration;
use typed_builder::TypedBuilder;

/// Argument builder for the Service Action admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::service_action`](crate::madmin::madmin_client::MadminClient::service_action) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ServiceAction {
    #[builder(!default)]
    client: MadminClient,
    /// The service action to perform
    #[builder(!default, setter(into, doc = "Action"))]
    action: ServiceActionType,
    /// Whether to perform a dry run (default: false)
    #[builder(default = false, setter(into, doc = "Dry run flag"))]
    dry_run: bool,
    /// Enable rolling restart (default: false)
    #[builder(default = false, setter(into, doc = "Rolling restart flag"))]
    rolling: bool,
    /// Graceful wait period for rolling restart (default: 0)
    #[builder(default, setter(into, doc = "Graceful wait duration"))]
    rolling_graceful_wait: Option<Duration>,
    /// Execute action by node instead of cluster-wide (default: false)
    #[builder(default = false, setter(into, doc = "By-node execution flag"))]
    by_node: bool,
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
}

/// Builder type for [`ServiceAction`].
pub type ServiceActionBldr = ServiceActionBuilder<(
    (MadminClient,),
    (ServiceActionType,),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

impl ToMadminRequest for ServiceAction {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("action", self.action.to_string());
        query_params.add("dry-run", self.dry_run.to_string());
        query_params.add("by-node", self.by_node.to_string());
        query_params.add("type", "2");

        if self.rolling {
            let wait_nanos = self
                .rolling_graceful_wait
                .unwrap_or(Duration::ZERO)
                .as_nanos() as i64;
            query_params.add("wait", wait_nanos.to_string());
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/service")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for ServiceAction {
    type MadminResponse = ServiceActionResponse;
}
