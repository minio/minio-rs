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
use crate::madmin::response::ListAccessKeysOpenIDBulkResponse;
use crate::madmin::types::openid::{ListAccessKeysOpts, ListType};
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the ListAccessKeysOpenIDBulk admin API operation.
///
/// Retrieves access keys and STS credentials for OpenID-authenticated users.
///
/// This struct constructs the parameters required for the [`MadminClient::list_access_keys_openid_bulk`] method.
///
/// # Example
///
/// ```no_run
/// use minio::madmin::madmin_client::MadminClient;
/// use minio::madmin::types::MadminApi;
/// use minio::madmin::types::openid::{ListAccessKeysOpts, ListType};
/// use minio::s3::creds::StaticProvider;
/// use minio::s3::http::BaseUrl;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let base_url: BaseUrl = "http://localhost:9000".parse()?;
///     let provider = StaticProvider::new("minioadmin", "minioadmin", None);
///     let madmin_client = MadminClient::new(base_url, Some(provider));
///
///     let opts = ListAccessKeysOpts {
///         list_type: Some(ListType::All),
///         config_name: Some("default".to_string()),
///         all_configs: None,
///         all: Some(true),
///     };
///
///     let response = madmin_client
///         .list_access_keys_openid_bulk()
///         .users(vec!["user1".to_string(), "user2".to_string()])
///         .opts(opts)
///         .build()
///         .send()
///         .await?;
///
///     for config_resp in &response.configs {
///         println!("Config: {}", config_resp.config_name);
///         for user in &config_resp.users {
///             println!("  User: {} ({})", user.readable_name, user.id);
///         }
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ListAccessKeysOpenIDBulk {
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
    #[builder(default, setter(into, doc = "List of user identifiers to query"))]
    users: Vec<String>,
    #[builder(default, setter(doc = "Options for listing access keys"))]
    opts: ListAccessKeysOpts,
}

pub type ListAccessKeysOpenIDBulkBldr =
    ListAccessKeysOpenIDBulkBuilder<((MadminClient,), (), (), (), ())>;

impl ToMadminRequest for ListAccessKeysOpenIDBulk {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();

        // Add users if specified
        for user in &self.users {
            query_params.add("users", user);
        }

        // Add list type if specified
        if let Some(list_type) = self.opts.list_type {
            let type_str = match list_type {
                ListType::All => "all",
                ListType::Sts => "sts",
                ListType::ServiceAccount => "serviceaccount",
            };
            query_params.add("listType", type_str);
        }

        // Add config name if specified
        if let Some(config_name) = &self.opts.config_name {
            query_params.add("configName", config_name);
        }

        // Add allConfigs flag if specified
        if let Some(all_configs) = self.opts.all_configs
            && all_configs
        {
            query_params.add("allConfigs", "true");
        }

        // Add all flag if specified
        if let Some(all) = self.opts.all
            && all
        {
            query_params.add("all", "true");
        }

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/idp/openid/list-access-keys-bulk")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(4)
            .build())
    }
}

impl MadminApi for ListAccessKeysOpenIDBulk {
    type MadminResponse = ListAccessKeysOpenIDBulkResponse;
}
