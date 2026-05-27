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
use crate::madmin::response::UpdateGroupMembersResponse;
use crate::madmin::types::group::GroupAddRemove;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the Update Group Members admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::update_group_members`](crate::madmin::madmin_client::MadminClient::update_group_members) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct UpdateGroupMembers {
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
    #[builder(setter(into, doc = "Policy association request details"))]
    request: GroupAddRemove,
}

/// Builder type for [`UpdateGroupMembers`].
pub type UpdateGroupMembersBldr = UpdateGroupMembersBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for UpdateGroupMembers {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        self.request.validate().map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: e,
                source: None,
            })
        })?;

        let json_data = serde_json::to_vec(&self.request)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(json_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/update-group-members")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for UpdateGroupMembers {
    type MadminResponse = UpdateGroupMembersResponse;
}
