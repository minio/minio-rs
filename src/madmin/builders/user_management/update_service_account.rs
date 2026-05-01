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
use crate::madmin::response::UpdateServiceAccountResponse;
use crate::madmin::types::service_account::UpdateServiceAccountReq;
use crate::madmin::types::typed_parameters::AccessKey;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the Update Service Account admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::update_service_account`](crate::madmin::madmin_client::MadminClient::update_service_account) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct UpdateServiceAccount {
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
    #[builder(!default, setter(into, doc = "Access key for the account"))]
    access_key: AccessKey,
    #[builder(setter(into, doc = "Policy association request details"))]
    request: UpdateServiceAccountReq,
}

/// Builder type for [`UpdateServiceAccount`].
pub type UpdateServiceAccountBldr = UpdateServiceAccountBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for UpdateServiceAccount {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        self.request.validate().map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: e,
                source: None,
            })
        })?;

        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("accessKey", self.access_key.into_inner());

        let json_data = serde_json::to_vec(&self.request)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        let password = self
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for UpdateServiceAccount".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let encrypted_data = crate::madmin::encrypt::encrypt_data(&password, &json_data)?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/update-service-account")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for UpdateServiceAccount {
    type MadminResponse = UpdateServiceAccountResponse;
}
