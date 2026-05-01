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
use crate::madmin::response::GetPolicyResponse;
use crate::madmin::types::typed_parameters::PolicyName;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the GetPolicy admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::get_policy`](crate::madmin::madmin_client::MadminClient::get_policy) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct GetPolicy {
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
    #[builder(!default, setter(into, doc = "Name of the KMS policy to retrieve"))]
    policy_name: PolicyName,
}

/// Builder type for [`GetPolicy`].
pub type GetPolicyBldr = GetPolicyBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for GetPolicy {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("name", self.policy_name.into_inner());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/minio/kms/v1/policy/read")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for GetPolicy {
    type MadminResponse = GetPolicyResponse;
}
