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
use crate::madmin::response::RebalanceStopResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [RebalanceStop](https://pkg.go.dev/github.com/minio/madmin-go/v3#AdminClient.RebalanceStop) admin API operation.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct RebalanceStop {
    #[builder(!default)]
    client: MadminClient,
    #[builder(default, setter(into, doc = "Optional extra HTTP headers"))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into, doc = "Optional extra query parameters"))]
    extra_query_params: Option<Multimap>,
}

pub type RebalanceStopBldr = RebalanceStopBuilder<((MadminClient,), (), ())>;

impl ToMadminRequest for RebalanceStop {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/rebalance/stop")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .build())
    }
}

impl MadminApi for RebalanceStop {
    type MadminResponse = RebalanceStopResponse;
}
