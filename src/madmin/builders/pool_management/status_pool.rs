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
use crate::madmin::response::pool_management::StatusPoolResponse;
use crate::madmin::types::{MadminApi, MadminRequest, PoolName, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [Status Pool](https://github.com/minio/madmin-go/blob/main/decommission-commands.go) admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::status_pool`](crate::madmin::madmin_client::MadminClient::status_pool) method.
///
/// # Example
///
/// ```no_run
/// use minio::s3::client::Client;
/// use minio::s3::creds::StaticProvider;
/// use minio::s3::http::BaseUrl;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
///     let client = Client::new(base_url, Some(Box::new(provider)), None, None)?;
///     let madmin = client.madmin();
///
///     let status = madmin
///         .status_pool("http://server{1...4}/disk{1...4}")?
///         .send()
///         .await?;
///
///     println!("Pool ID: {}", status.id);
///     if let Some(decom) = status.decommission {
///         println!("Decommissioning progress: {:.2}%", decom.percent_complete());
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, TypedBuilder)]
#[builder(doc)]
pub struct StatusPool {
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
    #[builder(!default, setter(into, doc = "Pool definition (e.g., \"http://server{1...4}/disk{1...4}\")"))]
    pool: PoolName,
}

/// Builder type for [`StatusPool`].
pub type StatusPoolBldr = StatusPoolBuilder<((MadminClient,), (), (), (PoolName,))>;

impl ToMadminRequest for StatusPool {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("pool", self.pool.into_inner());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/pools/status")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .build())
    }
}

impl MadminApi for StatusPool {
    type MadminResponse = StatusPoolResponse;
}
