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
use crate::madmin::response::AddServiceAccountResponse;
use crate::madmin::types::service_account::AddServiceAccountReq;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the Add Service Account admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::add_service_account`](crate::madmin::madmin_client::MadminClient::add_service_account) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct AddServiceAccount {
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
    #[builder(setter(
        into,
        doc = "Configuration options for service account creation.\n\nContains the following fields:\n- `policy`: Parsed IAM policy restricting permissions\n- `target_user`: Parent user for the service account\n- `access_key`: Optional custom access key\n- `secret_key`: Optional custom secret key\n- `name`: Display name (max 32 chars, alphanumeric/underscore/hyphen)\n- `description`: Account description (max 256 bytes)\n- `expiration`: Optional credential expiration timestamp"
    ))]
    request: AddServiceAccountReq,
}

/// Builder type for [`AddServiceAccount`].
pub type AddServiceAccountBldr = AddServiceAccountBuilder<((MadminClient,), (), (), ())>;

impl ToMadminRequest for AddServiceAccount {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        self.request.validate().map_err(|e| {
            Error::Validation(ValidationErr::StrError {
                message: e,
                source: None,
            })
        })?;

        let json_data = serde_json::to_vec(&self.request)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        let password = self
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for AddServiceAccount".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let encrypted_data = crate::madmin::encrypt::encrypt_data(&password, &json_data)?;
        let body = Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/add-service-account")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for AddServiceAccount {
    type MadminResponse = AddServiceAccountResponse;
}
