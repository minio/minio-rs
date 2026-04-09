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
use crate::madmin::response::ServerUpdateResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use std::time::Duration;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ServerUpdate {
    #[builder(!default)]
    client: MadminClient,
    #[builder(default, setter(into, doc = "Update URL"))]
    update_url: Option<String>,
    #[builder(default = false, setter(into, doc = "Dry run"))]
    dry_run: bool,
    #[builder(default = false, setter(into, doc = "Rolling update"))]
    rolling: bool,
    #[builder(default, setter(into, doc = "Rolling graceful wait"))]
    rolling_graceful_wait: Option<Duration>,
    #[builder(default = false, setter(into, doc = "By node"))]
    by_node: bool,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
}

pub type ServerUpdateBldr = ServerUpdateBuilder<((MadminClient,), (), (), (), (), (), (), ())>;

impl ToMadminRequest for ServerUpdate {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("type", "2");
        query_params.add("updateURL", self.update_url.unwrap_or_default());
        query_params.add("dry-run", self.dry_run.to_string());
        query_params.add("by-node", self.by_node.to_string());
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
            .path("/update")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for ServerUpdate {
    type MadminResponse = ServerUpdateResponse;
}
