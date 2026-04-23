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
use crate::madmin::response::profiling::DownloadProfilingDataResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the Download Profiling Data admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::download_profiling_data`](crate::madmin::madmin_client::MadminClient::download_profiling_data) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct DownloadProfilingDataOp {
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
}

/// Builder type for [`DownloadProfilingDataOp`].
pub type DownloadProfilingDataOpBldr = DownloadProfilingDataOpBuilder<((MadminClient,), (), ())>;

impl ToMadminRequest for DownloadProfilingDataOp {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/minio/admin/v3/profiling/download")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for DownloadProfilingDataOp {
    type MadminResponse = DownloadProfilingDataResponse;
}
