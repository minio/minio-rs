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
use crate::madmin::response::ListAccessKeysBulkResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListAccessKeysBulkReq {
    users: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    list_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    all: Option<bool>,
}

/// Argument builder for the List Access Keys Bulk admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::list_access_keys_bulk`](crate::madmin::madmin_client::MadminClient::list_access_keys_bulk) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ListAccessKeysBulk {
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

    #[builder(setter(into, doc = "Target user list to query access keys for"))]
    users: Vec<String>,

    #[builder(
        default,
        setter(
            into,
            doc = "Filter type for access keys.\n\nSupported values:\n- `\"users-only\"`: List only regular user keys\n- `\"sts-only\"`: List only STS (temporary) keys\n- `\"svcacc-only\"`: List only service account keys\n- `\"all\"`: List all key types\n\nWhen `None`, defaults to server's default behavior."
        )
    )]
    list_type: Option<String>,

    #[builder(
        default,
        setter(
            doc = "Include all users when true.\n\nWhen set to `true`, queries access keys across all users.\nWhen `false` (default), only queries for users specified in the `users` field."
        )
    )]
    all: bool,
}

/// Builder type for [`ListAccessKeysBulk`].
pub type ListAccessKeysBulkBldr = ListAccessKeysBulkBuilder<((MadminClient,), (), (), (), (), ())>;

impl ToMadminRequest for ListAccessKeysBulk {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let req_body = ListAccessKeysBulkReq {
            users: self.users,
            list_type: self.list_type,
            all: if self.all { Some(true) } else { None },
        };

        let json_data = serde_json::to_vec(&req_body)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        let password = self
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for ListAccessKeysBulk".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let encrypted_data = crate::madmin::encrypt::encrypt_data(&password, &json_data)?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data)));

        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("bulk", "");

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/list-access-keys")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for ListAccessKeysBulk {
    type MadminResponse = ListAccessKeysBulkResponse;
}
