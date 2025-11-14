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
use crate::madmin::response::SetKmsPolicyResponse;
use crate::madmin::types::typed_parameters::PolicyName;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the SetKmsPolicy admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::set_kms_policy`](crate::madmin::madmin_client::MadminClient::set_kms_policy) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SetKmsPolicy {
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
    #[builder(!default, setter(into, doc = "Name of the KMS policy"))]
    policy_name: PolicyName,
    #[builder(setter(doc = "Policy content as bytes"))]
    content: Vec<u8>,
}

/// Builder type for [`SetKmsPolicy`].
pub type SetKmsPolicyBldr = SetKmsPolicyBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for SetKmsPolicy {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("name", self.policy_name.into_inner());

        let body = Arc::new(SegmentedBytes::from(Bytes::from(self.content)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/minio/kms/v1/policy/write")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for SetKmsPolicy {
    type MadminResponse = SetKmsPolicyResponse;
}
