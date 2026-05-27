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
use crate::madmin::response::AddAzureCannedPolicyResponse;
use crate::madmin::types::policy::AddAzureCannedPolicyReq;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct AddAzureCannedPolicy {
    #[builder(!default)]
    client: MadminClient,
    #[builder(default, setter(into, doc = "Optional extra HTTP headers"))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into, doc = "Optional extra query parameters"))]
    extra_query_params: Option<Multimap>,
    #[builder(setter(doc = "Azure policy request"))]
    request: AddAzureCannedPolicyReq,
}

pub type AddAzureCannedPolicyBldr = AddAzureCannedPolicyBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for AddAzureCannedPolicy {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let json_data = serde_json::to_vec(&self.request)
            .map_err(crate::s3::error::ValidationErr::JsonError)?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(json_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/idp/openid/add-azure-canned-policy")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .body(Some(body))
            .build())
    }
}

impl MadminApi for AddAzureCannedPolicy {
    type MadminResponse = AddAzureCannedPolicyResponse;
}
