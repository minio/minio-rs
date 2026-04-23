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
use crate::madmin::response::ServerHealthInfoResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use std::time::Duration;
use typed_builder::TypedBuilder;

/// Argument builder for the Server Health Info admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::server_health_info`](crate::madmin::madmin_client::MadminClient::server_health_info) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ServerHealthInfo {
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
    #[builder(default, setter(into, doc = "Deadline"))]
    deadline: Option<Duration>,
    #[builder(default, setter(into, doc = "Anonymize"))]
    anonymize: Option<String>,
    /// Minio info
    #[builder(default = false)]
    minio_info: bool,
    /// Minio config
    #[builder(default = false)]
    minio_config: bool,
    /// Sys cpu
    #[builder(default = false)]
    sys_cpu: bool,
    /// Sys drive hw
    #[builder(default = false)]
    sys_drive_hw: bool,
    /// Sys os info
    #[builder(default = false)]
    sys_os_info: bool,
    /// Sys mem
    #[builder(default = false)]
    sys_mem: bool,
    /// Sys net
    #[builder(default = false)]
    sys_net: bool,
    /// Sys process
    #[builder(default = false)]
    sys_process: bool,
    /// Sys errors
    #[builder(default = false)]
    sys_errors: bool,
    /// Sys services
    #[builder(default = false)]
    sys_services: bool,
    /// Sys config
    #[builder(default = false)]
    sys_config: bool,
    /// Replication
    #[builder(default = false)]
    replication: bool,
    /// Shards health
    #[builder(default = false)]
    shards_health: bool,
}

/// Builder type for [`ServerHealthInfo`].
pub type ServerHealthInfoBldr = ServerHealthInfoBuilder<(
    (MadminClient,),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

impl MadminApi for ServerHealthInfo {
    type MadminResponse = ServerHealthInfoResponse;
}

impl ToMadminRequest for ServerHealthInfo {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();

        if let Some(deadline) = self.deadline {
            query_params.add("deadline", format!("{}s", deadline.as_secs()));
        }

        if let Some(anonymize) = self.anonymize {
            query_params.add("anonymize", anonymize);
        }

        if self.minio_info {
            query_params.add("minioinfo", "true");
        }
        if self.minio_config {
            query_params.add("minioconfig", "true");
        }
        if self.sys_cpu {
            query_params.add("syscpu", "true");
        }
        if self.sys_drive_hw {
            query_params.add("sysdrivehw", "true");
        }
        if self.sys_os_info {
            query_params.add("sysosinfo", "true");
        }
        if self.sys_mem {
            query_params.add("sysmem", "true");
        }
        if self.sys_net {
            query_params.add("sysnet", "true");
        }
        if self.sys_process {
            query_params.add("sysprocess", "true");
        }
        if self.sys_errors {
            query_params.add("syserrors", "true");
        }
        if self.sys_services {
            query_params.add("sysservices", "true");
        }
        if self.sys_config {
            query_params.add("sysconfig", "true");
        }
        if self.replication {
            query_params.add("replication", "true");
        }
        if self.shards_health {
            query_params.add("shardshealth", "true");
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/healthinfo")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}
