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
use crate::madmin::response::SetUserReqResponse;
use crate::madmin::types::typed_parameters::AccessKey;
use crate::madmin::types::user::AddOrUpdateUserReq;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the SetUserReq admin API operation.
///
/// Updates user credentials, status, and policies using a request object.
/// This is a more flexible alternative to SetUser that allows updating
/// multiple properties in a single operation.
///
/// This struct constructs the parameters required for the [`MadminClient::set_user_req`] method.
///
/// # Example
///
/// ```no_run
/// use minio::madmin::madmin_client::MadminClient;
/// use minio::madmin::types::MadminApi;
/// use minio::madmin::types::user::{AddOrUpdateUserReq, AccountStatus};
/// use minio::s3::creds::StaticProvider;
/// use minio::s3::http::BaseUrl;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let base_url: BaseUrl = "http://localhost:9000".parse()?;
///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
///     let madmin_client = MadminClient::new(base_url, Some(provider));
///
///     let req = AddOrUpdateUserReq {
///         secret_key: Some("new-secret-key".to_string()),
///         policy: Some("readwrite".to_string()),
///         status: AccountStatus::Enabled,
///     };
///
///     madmin_client
///         .set_user_req()
///         .access_key("test-user".to_string())
///         .request(req)
///         .build()
///         .send()
///         .await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct SetUserReq {
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
    #[builder(!default, setter(into, doc = "Access key of the user to update"))]
    access_key: AccessKey,
    #[builder(setter(doc = "User update request with new credentials/status/policy"))]
    request: AddOrUpdateUserReq,
}

pub type SetUserReqBldr = SetUserReqBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for SetUserReq {
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

        // Marshal to JSON
        let json_data = serde_json::to_vec(&self.request)
            .map_err(crate::s3::error::ValidationErr::JsonError)?;

        // Encrypt the JSON data
        let encrypted_data = crate::madmin::encrypt::encrypt_data(&admin_secret_key, &json_data)?;

        let body = Arc::new(SegmentedBytes::from(Bytes::from(encrypted_data)));

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/add-user")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .body(Some(body))
            .build())
    }
}

impl MadminApi for SetUserReq {
    type MadminResponse = SetUserReqResponse;
}
