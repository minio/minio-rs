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
use crate::madmin::response::TopLocksResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [Top Locks](https://min.io/docs/minio/linux/reference/minio-mc-admin/mc-admin-top-locks.html) admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::top_locks`](crate::madmin::madmin_client::MadminClient::top_locks) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct TopLocks {
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
    /// Set the number of locks to return (default: 10)
    #[builder(default = 10, setter(into, doc = "Count"))]
    count: i32,
    /// Include stale locks in the results (default: false)
    #[builder(default = false, setter(into, doc = "Stale"))]
    stale: bool,
}

/// Builder type for [`TopLocks`].
pub type TopLocksBldr = TopLocksBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for TopLocks {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("count", self.count.to_string());
        query_params.add("stale", self.stale.to_string());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/top/locks")
            .api_version(4)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for TopLocks {
    type MadminResponse = TopLocksResponse;
}
