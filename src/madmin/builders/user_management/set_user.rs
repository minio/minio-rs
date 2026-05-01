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
use crate::madmin::response::SetUserResponse;
use crate::madmin::types::typed_parameters::{AccessKey, SecretKey};
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SetUserReq {
    secret_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy: Option<Value>,
    status: String,
}

/// Argument builder for the Set User admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::set_user`](crate::madmin::madmin_client::MadminClient::set_user) method.
/// Used to create a new user or update an existing user's credentials, status, and policy.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SetUser {
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
    #[builder(setter(into, doc = "Access key for the account"))]
    access_key: AccessKey,
    #[builder(setter(into, doc = "Secret key for the account"))]
    secret_key: SecretKey,
    #[builder(default = "enabled".to_string(), setter(into, doc = "Status to set (enabled or disabled)"))]
    status: String,
    #[builder(default, setter(into, doc = "Policy document content"))]
    policy: Option<Value>,
}

/// Builder type for [`SetUser`].
pub type SetUserBldr = SetUserBuilder<((MadminClient,), (), (), (AccessKey,), (SecretKey,), (), ())>;

impl ToMadminRequest for SetUser {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("accessKey", self.access_key.into_inner());

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

        let req = SetUserReq {
            secret_key: self.secret_key.into_inner(),
            policy: self.policy,
            status: self.status,
        };

        let json_data =
            serde_json::to_vec(&req).map_err(crate::s3::error::ValidationErr::JsonError)?;

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

impl MadminApi for SetUser {
    type MadminResponse = SetUserResponse;
}
