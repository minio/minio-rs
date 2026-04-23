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

use crate::madmin::encrypt::encrypt_data;
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::AddUserResponse;
use crate::madmin::types::typed_parameters::{AccessKey, SecretKey};
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use serde::Serialize;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AddOrUpdateUserReq {
    secret_key: String,
    status: String,
}

/// Argument builder for the Add User admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::add_user`](crate::madmin::madmin_client::MadminClient::add_user) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct AddUser {
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
    #[builder(setter(into, doc = "Access key (username) for the new user"))]
    access_key: AccessKey,
    #[builder(setter(into, doc = "Secret key (password) for the new user"))]
    secret_key: SecretKey,
}

/// Builder type for [`AddUser`].
pub type AddUserBldr = AddUserBuilder<((MadminClient,), (), (), (AccessKey,), (SecretKey,))>;

impl ToMadminRequest for AddUser {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("accessKey", self.access_key.into_inner());

        // Get admin secret key for encryption
        let admin_secret_key = if let Some(provider) = &self.client.shared.provider {
            let creds = provider.fetch();
            creds.secret_key
        } else {
            return Err(Error::Validation(
                crate::s3::error::ValidationErr::StrError {
                    message: "No credentials provider available for encryption".to_string(),
                    source: None,
                },
            ));
        };

        // Create request payload
        let req = AddOrUpdateUserReq {
            secret_key: self.secret_key.into_inner(),
            status: "enabled".to_string(),
        };

        // Marshal to JSON
        let json_data =
            serde_json::to_vec(&req).map_err(crate::s3::error::ValidationErr::JsonError)?;

        // Encrypt the JSON data
        let encrypted_data = encrypt_data(&admin_secret_key, &json_data)?;

        let body = Arc::new(SegmentedBytes::from(bytes::Bytes::from(encrypted_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/add-user")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body))
            .build())
    }
}

impl MadminApi for AddUser {
    type MadminResponse = AddUserResponse;
}
