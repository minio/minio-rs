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
use crate::madmin::response::AddCannedPolicyResponse;
use crate::madmin::types::typed_parameters::PolicyName;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct AddCannedPolicy {
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
    #[builder(!default, setter(into, doc = "Name of the IAM policy to create"))]
    policy_name: PolicyName,
    #[builder(setter(into, doc = "Policy document as JSON bytes"))]
    policy: Vec<u8>,
}

/// Builder type for [`AddCannedPolicy`].
pub type AddCannedPolicyBldr = AddCannedPolicyBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for AddCannedPolicy {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        if self.policy.is_empty() {
            return Err(Error::Validation(ValidationErr::StrError {
                message: "Policy content cannot be empty".to_string(),
                source: None,
            }));
        }

        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("name", self.policy_name.into_inner());

        let body = Arc::new(SegmentedBytes::from(Bytes::from(self.policy)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/add-canned-policy")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for AddCannedPolicy {
    type MadminResponse = AddCannedPolicyResponse;
}
