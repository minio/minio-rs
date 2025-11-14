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
use crate::madmin::response::SetGroupStatusResponse;
use crate::madmin::types::group::GroupStatus;
use crate::madmin::types::typed_parameters::GroupName;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SetGroupStatus {
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
    #[builder(!default, setter(into, doc = "Group"))]
    group: GroupName,
    #[builder(setter(into, doc = "Status to set (enabled or disabled)"))]
    status: GroupStatus,
}

/// Builder type for [`SetGroupStatus`].
pub type SetGroupStatusBldr = SetGroupStatusBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for SetGroupStatus {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("group", self.group.into_inner());
        query_params.add("status", self.status.as_str());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/set-group-status")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for SetGroupStatus {
    type MadminResponse = SetGroupStatusResponse;
}
